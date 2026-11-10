using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using CromosList.Data;
using CromosList.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Controllers;

[ApiController]
[Route("api/admin/transfermarkt")]
[Authorize]
public class TransfermarktController : ControllerBase
{
    private readonly CromosListDbContext _context;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConfiguration _configuration;

    public TransfermarktController(
        CromosListDbContext context,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration)
    {
        _context = context;
        _httpClientFactory = httpClientFactory;
        _configuration = configuration;
    }

    [HttpPost("plantilla")]
    public async Task<IActionResult> ImportarPlantilla(
        [FromBody] ImportarPlantillaDto dto,
        CancellationToken cancellationToken)
    {
        var usuarioId = ObtenerMiId();
        var esAdmin = await _context.Usuarios
            .AsNoTracking()
            .AnyAsync(u => u.Id == usuarioId && u.EsAdmin, cancellationToken);

        if (!esAdmin)
            return Forbid();

        if (dto.EquipoId <= 0)
            return BadRequest("Selecciona un equipo de CromosList.");

        var club = dto.Club?.Trim();
        if (string.IsNullOrWhiteSpace(club) || club.Length > 500)
            return BadRequest("Indica el ID o el enlace del club en Transfermarkt.");

        if (!string.IsNullOrWhiteSpace(dto.Temporada) && dto.Temporada.Trim().Length > 20)
            return BadRequest("La temporada no puede superar los 20 caracteres.");

        var equipo = await _context.Equipos
            .AsNoTracking()
            .FirstOrDefaultAsync(e => e.Id == dto.EquipoId, cancellationToken);

        if (equipo is null)
            return NotFound("El equipo seleccionado no existe.");

        var baseUrl = _configuration["TransfermarktScraper:BaseUrl"]?.Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(baseUrl) || !Uri.TryCreate(baseUrl, UriKind.Absolute, out var scraperUri))
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                "El scraper de Transfermarkt no está configurado. Define TransfermarktScraper__BaseUrl.");

        var temporadaSolicitada = string.IsNullOrWhiteSpace(dto.Temporada)
            ? ExtraerTemporadaDelEnlace(club)
            : dto.Temporada.Trim();

        var query = $"club={Uri.EscapeDataString(club)}";
        if (!string.IsNullOrWhiteSpace(temporadaSolicitada))
            query += $"&season={Uri.EscapeDataString(temporadaSolicitada)}";

        var requestUri = new Uri(scraperUri, $"clubs/squad?{query}");
        TransfermarktSquadDto? plantilla;

        try
        {
            using var response = await _httpClientFactory.CreateClient("TransfermarktScraper")
                .GetAsync(requestUri, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var detalle = (await response.Content.ReadAsStringAsync(cancellationToken)).Trim();
                if (detalle.Length > 300)
                    detalle = detalle[..300];

                return StatusCode(StatusCodes.Status502BadGateway,
                    $"El scraper respondió {(int)response.StatusCode}. {detalle}");
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            plantilla = await JsonSerializer.DeserializeAsync<TransfermarktSquadDto>(stream,
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return StatusCode(StatusCodes.Status504GatewayTimeout,
                "El scraper tardó demasiado en responder. Inténtalo de nuevo más tarde.");
        }
        catch (HttpRequestException)
        {
            return StatusCode(StatusCodes.Status502BadGateway,
                "No se pudo conectar con el scraper de Transfermarkt. Comprueba que esté disponible.");
        }
        catch (JsonException)
        {
            return StatusCode(StatusCodes.Status502BadGateway,
                "El scraper devolvió una respuesta que CromosList no pudo interpretar.");
        }

        if (plantilla?.Players is null)
            return StatusCode(StatusCodes.Status502BadGateway,
                "La respuesta del scraper no contiene una plantilla válida.");

        var temporada = ObtenerTemporada(plantilla.Season, temporadaSolicitada);
        var jugadores = await _context.Jugadores
            .Include(j => j.Equipos)
            .Include(j => j.Paises)
            .ToListAsync(cancellationToken);
        var porNombre = new Dictionary<string, Jugador>(StringComparer.OrdinalIgnoreCase);
        foreach (var existente in jugadores)
        {
            AgregarAlias(porNombre, existente.Nombre, existente);
            AgregarAlias(porNombre, existente.NombreCompleto, existente);
        }

        var paises = await _context.Paises.ToListAsync(cancellationToken);
        var paisPorNombre = new Dictionary<string, Pais>(StringComparer.OrdinalIgnoreCase);
        foreach (var pais in paises)
            paisPorNombre.TryAdd(pais.Nombre.Trim(), pais);
        var creados = 0;
        var vinculados = 0;
        var omitidos = 0;

        foreach (var fila in plantilla.Players)
        {
            var nombre = fila.Player?.Name?.Trim();
            if (string.IsNullOrWhiteSpace(nombre))
            {
                omitidos++;
                continue;
            }

            if (!porNombre.TryGetValue(nombre, out var jugador))
            {
                jugador = new Jugador
                {
                    Nombre = nombre,
                    NombreCompleto = fila.Player?.FullName?.Trim() ?? string.Empty,
                    Internacional = false
                };

                _context.Jugadores.Add(jugador);
                jugadores.Add(jugador);
                AgregarAlias(porNombre, jugador.Nombre, jugador);
                AgregarAlias(porNombre, jugador.NombreCompleto, jugador);
                creados++;
            }

            foreach (var nacionalidad in fila.Nationalities ?? [])
            {
                var nombrePais = nacionalidad.Name?.Trim();
                if (string.IsNullOrWhiteSpace(nombrePais))
                    continue;

                if (!paisPorNombre.TryGetValue(nombrePais, out var pais))
                {
                    pais = new Pais { Nombre = nombrePais };
                    _context.Paises.Add(pais);
                    paises.Add(pais);
                    paisPorNombre[nombrePais] = pais;
                }

                if (!jugador.Paises.Any(p => string.Equals(p.Nombre, nombrePais, StringComparison.OrdinalIgnoreCase)))
                    jugador.Paises.Add(pais);
            }

            var yaVinculado = jugador.Equipos.Any(e => e.EquipoId == equipo.Id &&
                string.Equals(e.TemporadaInicio, temporada, StringComparison.OrdinalIgnoreCase));
            if (!yaVinculado)
            {
                jugador.Equipos.Add(new JugadorEquipo
                {
                    Jugador = jugador,
                    EquipoId = equipo.Id,
                    TemporadaInicio = temporada,
                    TemporadaFin = temporada
                });
                vinculados++;
            }
        }

        if (vinculados == 0 && creados == 0)
            return Ok(new ImportarPlantillaResultado(0, 0, omitidos, temporada, equipo.Nombre));

        await _context.SaveChangesAsync(cancellationToken);
        return Ok(new ImportarPlantillaResultado(creados, vinculados, omitidos, temporada, equipo.Nombre));
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                  ?? User.FindFirst("sub")?.Value;

        return long.TryParse(sub, out var id) ? id : 0;
    }

    private static void AgregarAlias(IDictionary<string, Jugador> jugadores, string? alias, Jugador jugador)
    {
        if (!string.IsNullOrWhiteSpace(alias))
            jugadores.TryAdd(alias.Trim(), jugador);
    }

    private static string ObtenerTemporada(TransfermarktSeasonDto? temporada, string? solicitada)
    {
        if (int.TryParse(temporada?.Id?.ToString(), out var anio) && anio is >= 1900 and <= 2100)
            return $"{anio}/{anio + 1}";

        if (!string.IsNullOrWhiteSpace(temporada?.Name))
            return NormalizarTemporada(temporada.Name.Trim());

        if (!string.IsNullOrWhiteSpace(solicitada))
            return NormalizarTemporada(solicitada.Trim());

        return "Actualidad";
    }

    private static string? ExtraerTemporadaDelEnlace(string club)
    {
        if (!Uri.TryCreate(club, UriKind.Absolute, out var uri))
            return null;

        var coincidencia = Regex.Match(uri.AbsolutePath, @"/saison_id/(\d{4})(?:/|$)",
            RegexOptions.IgnoreCase);
        return coincidencia.Success ? coincidencia.Groups[1].Value : null;
    }

    private static string NormalizarTemporada(string temporada)
    {
        var coincidencia = Regex.Match(temporada, @"^(\d{4})(?:[/\-](\d{2}|\d{4}))?$");
        if (!coincidencia.Success || !int.TryParse(coincidencia.Groups[1].Value, out var inicio))
            return temporada;

        if (!coincidencia.Groups[2].Success)
            return $"{inicio}/{inicio + 1}";

        var finTexto = coincidencia.Groups[2].Value;
        if (!int.TryParse(finTexto, out var fin))
            return temporada;

        if (finTexto.Length == 2)
            fin = inicio / 100 * 100 + fin + (fin < inicio % 100 ? 100 : 0);

        return $"{inicio}/{fin}";
    }
}

public sealed class ImportarPlantillaDto
{
    public long EquipoId { get; set; }
    public string? Club { get; set; }
    public string? Temporada { get; set; }
}

public sealed record ImportarPlantillaResultado(
    int JugadoresCreados,
    int AsociacionesCreadas,
    int FilasOmitidas,
    string Temporada,
    string Equipo);

internal sealed class TransfermarktSquadDto
{
    [JsonPropertyName("season")]
    public TransfermarktSeasonDto? Season { get; set; }

    [JsonPropertyName("players")]
    public List<TransfermarktSquadRowDto>? Players { get; set; }
}

internal sealed class TransfermarktSeasonDto
{
    [JsonPropertyName("id")]
    public JsonElement? Id { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }
}

internal sealed class TransfermarktSquadRowDto
{
    [JsonPropertyName("player")]
    public TransfermarktPlayerDto? Player { get; set; }

    [JsonPropertyName("nationalities")]
    public List<TransfermarktNationalityDto>? Nationalities { get; set; }
}

internal sealed class TransfermarktPlayerDto
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("full_name")]
    public string? FullName { get; set; }
}

internal sealed class TransfermarktNationalityDto
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }
}
