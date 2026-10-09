using System.Globalization;
using System.Security.Claims;
using System.Text.RegularExpressions;
using CromosList.Data;
using CromosList.Models;
using CromosList.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class JugadoresController : ControllerBase
{
    private readonly CromosListDbContext _context;
    private readonly AlbumPersonalizadoService _albumPersonalizado;

    public JugadoresController(CromosListDbContext context, AlbumPersonalizadoService albumPersonalizado)
    {
        _context = context;
        _albumPersonalizado = albumPersonalizado;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll([FromQuery] long? equipoId)
    {
        var query = _context.Jugadores
            .AsNoTracking()
            .AsQueryable();

        if (equipoId.HasValue)
        {
            query = query.Where(j => j.Equipos.Any(je => je.EquipoId == equipoId.Value));
        }

        var jugadores = await query
            .OrderBy(j => j.Nombre)
            .Select(j => new
            {
                j.Id,
                j.Nombre,
                j.NombreCompleto,
                j.Internacional,
                Equipos = j.Equipos
                    .OrderBy(je => je.Equipo.Nombre)
                    .Select(je => new
                    {
                        je.Equipo.Id,
                        je.Equipo.Nombre,
                        je.TemporadaInicio,
                        je.TemporadaFin
                    })
                    .ToList(),
                AniosInternacional = j.AniosInternacional
                    .Select(a => a.Anio)
                    .OrderBy(a => a)
                    .ToList(),
                Paises = j.Paises
                    .OrderBy(p => p.Nombre)
                    .Select(p => new
                    {
                        p.Id,
                        p.Nombre
                    })
                    .ToList()
            })
            .ToListAsync();

        return Ok(jugadores);
    }

    [HttpGet("{id:long}")]
    public async Task<IActionResult> GetById(long id)
    {
        var jugador = await _context.Jugadores
            .AsNoTracking()
            .Where(j => j.Id == id)
            .Select(j => new
            {
                j.Id,
                j.Nombre,
                j.NombreCompleto,
                j.Internacional,
                Equipos = j.Equipos
                    .OrderBy(je => je.Equipo.Nombre)
                    .Select(je => new
                    {
                        je.Equipo.Id,
                        je.Equipo.Nombre,
                        je.TemporadaInicio,
                        je.TemporadaFin
                    })
                    .ToList(),
                AniosInternacional = j.AniosInternacional
                    .Select(a => a.Anio)
                    .OrderBy(a => a)
                    .ToList(),
                Paises = j.Paises
                    .OrderBy(p => p.Nombre)
                    .Select(p => new
                    {
                        p.Id,
                        p.Nombre
                    })
                    .ToList()
            })
            .FirstOrDefaultAsync();

        if (jugador is null)
            return NotFound();

        return Ok(jugador);
    }

    // Cromos de un jugador agrupados por álbum (colección), ordenados de mayor a menor dentro de cada uno.
    [HttpGet("{id:long}/Cromos")]
    public async Task<IActionResult> GetCromos(long id)
    {
        var jugador = await _context.Jugadores
            .AsNoTracking()
            .Where(j => j.Id == id)
            .Select(j => new { j.Id, j.Nombre })
            .FirstOrDefaultAsync();

        if (jugador is null)
            return NotFound();

        var cromos = await _context.Cromos
            .AsNoTracking()
            .Include(c => c.Album)
                .ThenInclude(a => a.Editorial)
            .Include(c => c.Equipo)
            .Include(c => c.Edicion)
            .Include(c => c.TipoCromo)
            .Where(c => c.JugadorId == id)
            .ToListAsync();

        var colecciones = cromos
            .GroupBy(c => c.AlbumId)
            .Select(g => new
            {
                Id = g.Key,
                Nombre = g.First().Album.Nombre,
                Temporada = g.First().Album.Temporada,
                Editorial = new { g.First().Album.Editorial.Id, g.First().Album.Editorial.Nombre },
                Cromos = g
                    .OrderByDescending(c => NumeroValor(c.Numero))
                    .Select(c => new
                    {
                        c.Id,
                        c.Numero,
                        Equipo = new { c.Equipo.Id, c.Equipo.Nombre },
                        Edicion = new { c.Edicion.Id, c.Edicion.Nombre },
                        TipoCromo = new { c.TipoCromo.Id, c.TipoCromo.Nombre }
                    })
                    .ToList()
            })
            .OrderBy(g => g.Nombre)
            .ToList();

        return Ok(new
        {
            jugador.Id,
            jugador.Nombre,
            TotalCromos = cromos.Count,
            Colecciones = colecciones
        });
    }

    // Crea un álbum nuevo e independiente con todos los cromos del jugador
    // y apunta al usuario autenticado a esa colección.
    [HttpPost("{id:long}/album-personalizado")]
    public async Task<IActionResult> CrearAlbumPersonalizado(long id)
    {
        var usuarioId = ObtenerMiId();
        if (usuarioId == 0)
            return Unauthorized();

        var jugador = await _context.Jugadores
            .AsNoTracking()
            .Where(j => j.Id == id)
            .Select(j => new { j.Id, j.Nombre })
            .FirstOrDefaultAsync();

        if (jugador is null)
            return NotFound("El jugador no existe.");

        var nombreAlbum = $"Álbum de {jugador.Nombre}";

        var (albumId, error) = await _albumPersonalizado.CrearAsync(
            usuarioId, nombreAlbum, "jugador", c => c.JugadorId == id);

        if (error is not null)
            return BadRequest(error);

        return Ok(new { albumId, nombre = nombreAlbum });
    }

    [HttpPost]
    public async Task<IActionResult> Crear([FromBody] JugadorDto dto)
    {
        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del jugador es obligatorio.");

        var (equipos, errorEquipos) = await ValidarEquipos(dto.Equipos);
        if (errorEquipos is not null)
            return BadRequest(errorEquipos);

        var (anios, errorAnios) = LimpiarAnios(dto.Internacional, dto.AniosInternacional);
        if (errorAnios is not null)
            return BadRequest(errorAnios);

        var paises = await ValidarPaises(dto.Paises);
        if (paises is null)
            return BadRequest("Debes indicar al menos un país válido.");

        var jugador = new Jugador
        {
            Nombre = dto.Nombre.Trim(),
            NombreCompleto = dto.NombreCompleto?.Trim() ?? string.Empty,
            Internacional = dto.Internacional,
            Equipos = equipos!,
            AniosInternacional = anios
                .Select(a => new JugadorAnioInternacional { Anio = a })
                .ToList(),
            Paises = paises
        };

        _context.Jugadores.Add(jugador);
        await _context.SaveChangesAsync();

        var creado = await Proyeccion(jugador.Id);
        return CreatedAtAction(nameof(GetById), new { id = jugador.Id }, creado);
    }

    [HttpPut("{id:long}")]
    public async Task<IActionResult> Editar(long id, [FromBody] JugadorDto dto)
    {
        var existente = await _context.Jugadores
            .Include(j => j.Equipos)
            .Include(j => j.AniosInternacional)
            .Include(j => j.Paises)
            .FirstOrDefaultAsync(j => j.Id == id);

        if (existente is null)
            return NotFound();

        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del jugador es obligatorio.");

        var (equipos, errorEquipos) = await ValidarEquipos(dto.Equipos);
        if (errorEquipos is not null)
            return BadRequest(errorEquipos);

        var (anios, errorAnios) = LimpiarAnios(dto.Internacional, dto.AniosInternacional);
        if (errorAnios is not null)
            return BadRequest(errorAnios);

        var paises = await ValidarPaises(dto.Paises);
        if (paises is null)
            return BadRequest("Debes indicar al menos un país válido.");

        existente.Nombre = dto.Nombre.Trim();
        existente.NombreCompleto = dto.NombreCompleto?.Trim() ?? string.Empty;
        existente.Internacional = dto.Internacional;

        existente.Equipos.Clear();
        foreach (var equipo in equipos!)
        {
            existente.Equipos.Add(new JugadorEquipo
            {
                JugadorId = existente.Id,
                EquipoId = equipo.EquipoId,
                TemporadaInicio = equipo.TemporadaInicio,
                TemporadaFin = equipo.TemporadaFin
            });
        }

        existente.AniosInternacional.Clear();
        foreach (var anio in anios)
        {
            existente.AniosInternacional.Add(new JugadorAnioInternacional
            {
                JugadorId = existente.Id,
                Anio = anio
            });
        }

        existente.Paises = paises;

        await _context.SaveChangesAsync();

        return Ok(await Proyeccion(existente.Id));
    }

    [HttpDelete("{id:long}")]
    public async Task<IActionResult> Eliminar(long id)
    {
        var jugador = await _context.Jugadores.FindAsync(id);

        if (jugador is null)
            return NotFound();

        _context.Jugadores.Remove(jugador);
        await _context.SaveChangesAsync();

        return NoContent();
    }

    private Task<object?> Proyeccion(long id)
    {
        return _context.Jugadores
            .AsNoTracking()
            .Where(j => j.Id == id)
            .Select(j => new
            {
                j.Id,
                j.Nombre,
                j.NombreCompleto,
                j.Internacional,
                Equipos = j.Equipos
                    .OrderBy(je => je.Equipo.Nombre)
                    .Select(je => new
                    {
                        je.Equipo.Id,
                        je.Equipo.Nombre,
                        je.TemporadaInicio,
                        je.TemporadaFin
                    })
                    .ToList(),
                AniosInternacional = j.AniosInternacional
                    .Select(a => a.Anio)
                    .OrderBy(a => a)
                    .ToList(),
                Paises = j.Paises
                    .OrderBy(p => p.Nombre)
                    .Select(p => new
                    {
                        p.Id,
                        p.Nombre
                    })
                    .ToList()
            })
            .Cast<object?>()
            .FirstOrDefaultAsync();
    }

    // Normaliza los años de internacionalidad: solo tiene sentido si el jugador
    // es internacional; en ese caso exige al menos un año.
    private static (List<int> Anios, string? Error) LimpiarAnios(bool internacional, List<int>? anios)
    {
        if (!internacional)
            return (new List<int>(), null);

        var limpios = (anios ?? new List<int>())
            .Where(a => a >= 1900 && a <= 2100)
            .Distinct()
            .OrderBy(a => a)
            .ToList();

        if (limpios.Count == 0)
            return (new List<int>(), "Debes indicar al menos un año de internacionalidad.");

        return (limpios, null);
    }

    // Valida y normaliza los equipos del jugador, cada uno con su temporada de
    // inicio (obligatoria) y de fin (opcional).
    private async Task<(List<JugadorEquipo>? Equipos, string? Error)> ValidarEquipos(List<JugadorEquipoDto>? equipos)
    {
        if (equipos is null || equipos.Count == 0)
            return (null, "Debes indicar al menos un equipo.");

        var normalizados = new List<JugadorEquipo>();
        var equipoIds = new List<long>();
        var etapas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var dto in equipos)
        {
            var inicio = (dto.TemporadaInicio ?? string.Empty).Trim();
            var fin = (dto.TemporadaFin ?? string.Empty).Trim();

            if (inicio.Length == 0)
                return (null, "Cada equipo necesita una temporada de inicio.");

            if (inicio.Length > 50 || fin.Length > 50)
                return (null, "Cada temporada no puede superar los 50 caracteres.");

            // Un jugador puede tener varias etapas en el mismo equipo, pero no
            // dos con la misma temporada de inicio (chocarían en la clave).
            if (!etapas.Add($"{dto.EquipoId}|{inicio}"))
                return (null, "Hay dos etapas del mismo equipo con la misma temporada de inicio.");

            if (!equipoIds.Contains(dto.EquipoId))
                equipoIds.Add(dto.EquipoId);

            normalizados.Add(new JugadorEquipo
            {
                EquipoId = dto.EquipoId,
                TemporadaInicio = inicio,
                TemporadaFin = fin
            });
        }

        var existentes = await _context.Equipos
            .Where(e => equipoIds.Contains(e.Id))
            .Select(e => e.Id)
            .ToListAsync();

        if (existentes.Count != equipoIds.Count)
            return (null, "Uno o más equipos especificados no existen.");

        return (normalizados, null);
    }

    private async Task<List<Pais>?> ValidarPaises(IReadOnlyCollection<long>? paisIds)
    {
        if (paisIds is null || paisIds.Count == 0)
            return null;

        var ids = paisIds.Distinct().ToList();
        var paises = await _context.Paises
            .Where(p => ids.Contains(p.Id))
            .ToListAsync();

        if (paises.Count != ids.Count)
            return null;

        return paises;
    }

    private static double NumeroValor(string? numero)
    {
        var match = Regex.Match(numero ?? string.Empty, @"\d+");
        return match.Success
            ? double.Parse(match.Value, CultureInfo.InvariantCulture)
            : double.MaxValue;
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                  ?? User.FindFirst("sub")?.Value;

        return long.TryParse(sub!, out var id) ? id : 0;
    }
}

public class JugadorDto
{
    public string Nombre { get; set; } = string.Empty;
    public string? NombreCompleto { get; set; }
    public List<JugadorEquipoDto> Equipos { get; set; } = new();
    public bool Internacional { get; set; }
    public List<int>? AniosInternacional { get; set; }
    public List<long>? Paises { get; set; }
}

public class JugadorEquipoDto
{
    public long EquipoId { get; set; }
    public string? TemporadaInicio { get; set; }
    public string? TemporadaFin { get; set; }
}
