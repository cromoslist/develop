using System.Security.Claims;
using System.Text;
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

        if (!string.IsNullOrWhiteSpace(dto.Temporada) && dto.Temporada.Trim().Length > 20)
            return BadRequest("La temporada no puede superar los 20 caracteres.");

        var equipo = await _context.Equipos
            .AsNoTracking()
            .FirstOrDefaultAsync(e => e.Id == dto.EquipoId, cancellationToken);

        if (equipo is null)
            return NotFound("El equipo seleccionado no existe.");

        var club = !string.IsNullOrWhiteSpace(equipo.TransfermarktId)
            ? equipo.TransfermarktId.Trim()
            : dto.Club?.Trim();
        if (string.IsNullOrWhiteSpace(club) || club.Length > 500)
            return BadRequest("El equipo no tiene un ID de Transfermarkt guardado. Indica el ID o el enlace del club.");

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
            using var request = CrearSolicitudScraper(requestUri);
            using var response = await _httpClientFactory.CreateClient("TransfermarktScraper")
                .SendAsync(request, cancellationToken);

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
        var porNombre = new Dictionary<string, List<Jugador>>(StringComparer.Ordinal);
        foreach (var existente in jugadores)
        {
            AgregarAlias(porNombre, existente.Nombre, existente);
            AgregarAlias(porNombre, existente.NombreCompleto, existente);
        }
        var porTransfermarktId = jugadores
            .Where(j => !string.IsNullOrWhiteSpace(j.TransfermarktId))
            .ToDictionary(j => j.TransfermarktId!, StringComparer.Ordinal);

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

            var transfermarktId = ObtenerIdTransfermarkt(fila.Player?.Id);
            Jugador? jugador = null;

            if (transfermarktId is not null)
                porTransfermarktId.TryGetValue(transfermarktId, out jugador);

            if (jugador is null)
            {
                var coincidencias = new HashSet<Jugador>();
                foreach (var alias in new[] { nombre, fila.Player?.FullName })
                {
                    var clave = NormalizarNombre(alias);
                    if (clave.Length == 0 || !porNombre.TryGetValue(clave, out var candidatos))
                        continue;

                    foreach (var candidato in candidatos)
                    {
                        if (transfermarktId is null
                            || string.IsNullOrWhiteSpace(candidato.TransfermarktId)
                            || string.Equals(candidato.TransfermarktId, transfermarktId, StringComparison.Ordinal))
                            coincidencias.Add(candidato);
                    }
                }

                // Solo reutiliza por nombre cuando hay una única coincidencia
                // compatible. Si hay homónimos, crea un registro separado.
                if (coincidencias.Count == 1)
                {
                    jugador = coincidencias.Single();
                }
                else
                {
                    jugador = new Jugador
                    {
                        Nombre = nombre,
                        NombreCompleto = fila.Player?.FullName?.Trim() ?? string.Empty,
                        TransfermarktId = transfermarktId,
                        Internacional = false
                    };

                    _context.Jugadores.Add(jugador);
                    jugadores.Add(jugador);
                    creados++;
                }
            }

            if (transfermarktId is not null)
            {
                jugador.TransfermarktId ??= transfermarktId;
                porTransfermarktId.TryAdd(transfermarktId, jugador);
            }
            AgregarAlias(porNombre, jugador.Nombre, jugador);
            AgregarAlias(porNombre, jugador.NombreCompleto, jugador);
            AgregarAlias(porNombre, nombre, jugador);
            AgregarAlias(porNombre, fila.Player?.FullName, jugador);

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

    private HttpRequestMessage CrearSolicitudScraper(Uri requestUri)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        var apiKey = _configuration["TransfermarktScraper:ApiKey"]?.Trim();
        if (!string.IsNullOrWhiteSpace(apiKey))
            request.Headers.TryAddWithoutValidation("X-API-Key", apiKey);

        return request;
    }

    private static void AgregarAlias(IDictionary<string, List<Jugador>> jugadores, string? alias, Jugador jugador)
    {
        var clave = NormalizarNombre(alias);
        if (clave.Length == 0)
            return;

        if (!jugadores.TryGetValue(clave, out var coincidencias))
        {
            coincidencias = [];
            jugadores[clave] = coincidencias;
        }

        if (!coincidencias.Contains(jugador))
            coincidencias.Add(jugador);
    }

    private static string NormalizarNombre(string? nombre)
    {
        if (string.IsNullOrWhiteSpace(nombre))
            return string.Empty;

        var sinTildes = Regex.Replace(
            nombre.Trim().ToLowerInvariant().Normalize(NormalizationForm.FormD),
            @"\p{Mn}",
            string.Empty);
        return Regex.Replace(sinTildes, @"\s+", " ").Normalize(NormalizationForm.FormC);
    }

    private static string? ObtenerIdTransfermarkt(JsonElement? id)
    {
        if (id is null || id.Value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        var valor = id.Value.ToString().Trim();
        return string.IsNullOrWhiteSpace(valor) ? null : valor;
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
    [JsonPropertyName("id")]
    public JsonElement? Id { get; set; }

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
