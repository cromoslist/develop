using System.Globalization;
using System.Security.Claims;
using System.Text.RegularExpressions;
using CromosList.Data;
using CromosList.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class JugadoresController : ControllerBase
{
    // Marca de los álbumes creados por un usuario para sus jugadores.
    private const string TemporadaAlbumPersonalizado = "Personalizado";

    private readonly CromosListDbContext _context;

    public JugadoresController(CromosListDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll([FromQuery] long? equipoId)
    {
        var query = _context.Jugadores
            .AsNoTracking()
            .AsQueryable();

        if (equipoId.HasValue)
        {
            query = query.Where(j => j.Equipos.Any(e => e.Id == equipoId.Value));
        }

        var jugadores = await query
            .OrderBy(j => j.Nombre)
            .Select(j => new
            {
                j.Id,
                j.Nombre,
                j.NombreCompleto,
                Equipos = j.Equipos
                    .OrderBy(e => e.Nombre)
                    .Select(e => new
                    {
                        e.Id,
                        e.Nombre
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
                Equipos = j.Equipos
                    .OrderBy(e => e.Nombre)
                    .Select(e => new
                    {
                        e.Id,
                        e.Nombre
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

        var cromos = await _context.Cromos
            .AsNoTracking()
            .Where(c => c.JugadorId == id)
            .Select(c => new
            {
                c.Numero,
                c.JugadorId,
                c.EquipoId,
                c.EdicionId,
                c.PublicacionId,
                c.TipoCromoId,
                c.ColeccionId,
                c.AlbumId
            })
            .ToListAsync();

        if (cromos.Count == 0)
            return BadRequest("El jugador no tiene cromos todavía.");

        var nombreAlbum = $"Álbum de {jugador.Nombre}";

        // Reutilizar solo el álbum personal del propio usuario: los álbumes
        // personalizados de otros usuarios no son visibles ni reutilizables.
        var albumId = await _context.Albums
            .AsNoTracking()
            .Where(a => a.Nombre == nombreAlbum
                        && a.Temporada == TemporadaAlbumPersonalizado
                        && _context.UsuariosAlbumes.Any(ua => ua.AlbumId == a.Id && ua.UsuarioId == usuarioId))
            .Select(a => (long?)a.Id)
            .FirstOrDefaultAsync();

        if (albumId is null)
        {
            var editorial = await _context.Editoriales
                .FirstOrDefaultAsync(e => e.Nombre == "Personalizado");

            if (editorial is null)
            {
                editorial = new Editorial { Nombre = "Personalizado" };
                _context.Editoriales.Add(editorial);
                await _context.SaveChangesAsync();
            }

            var album = new Album
            {
                Nombre = nombreAlbum,
                Temporada = TemporadaAlbumPersonalizado,
                EditorialId = editorial.Id
            };

            _context.Albums.Add(album);
            await _context.SaveChangesAsync();

            var equipoIds = cromos
                .Select(c => c.EquipoId)
                .Distinct()
                .ToList();

            for (int i = 0; i < equipoIds.Count; i++)
            {
                _context.AlbumEquipos.Add(new AlbumEquipo
                {
                    AlbumId = album.Id,
                    EquipoId = equipoIds[i],
                    Orden = i
                });
            }

            var tipoIds = cromos
                .Select(c => c.TipoCromoId)
                .Distinct()
                .ToList();

            for (int i = 0; i < tipoIds.Count; i++)
            {
                _context.AlbumTiposCromo.Add(new AlbumTipoCromo
                {
                    AlbumId = album.Id,
                    TipoCromoId = tipoIds[i],
                    Orden = i
                });
            }

            // Colección de origen de cada cromo: se conserva el álbum del que
            // procede como una colección reutilizable por nombre.
            var coleccionPorAlbum = new Dictionary<long, long>();
            var albumesOrigen = cromos
                .Select(c => c.AlbumId)
                .Distinct()
                .ToList();

            var albumesOrigenInfo = await _context.Albums
                .AsNoTracking()
                .Where(a => albumesOrigen.Contains(a.Id))
                .Select(a => new { a.Id, a.Nombre })
                .ToListAsync();

            foreach (var alb in albumesOrigenInfo)
            {
                var coleccion = await _context.Colecciones
                    .FirstOrDefaultAsync(col => col.Nombre == alb.Nombre);

                if (coleccion is null)
                {
                    coleccion = new Coleccion { Nombre = alb.Nombre };
                    _context.Colecciones.Add(coleccion);
                    await _context.SaveChangesAsync();
                }

                coleccionPorAlbum[alb.Id] = coleccion.Id;
            }

            foreach (var c in cromos)
            {
                _context.Cromos.Add(new Cromo
                {
                    Numero = c.Numero,
                    JugadorId = c.JugadorId,
                    EquipoId = c.EquipoId,
                    EdicionId = c.EdicionId,
                    PublicacionId = c.PublicacionId,
                    TipoCromoId = c.TipoCromoId,
                    ColeccionId = coleccionPorAlbum.TryGetValue(c.AlbumId, out var colId) ? colId : null,
                    AlbumId = album.Id
                });
            }

            await _context.SaveChangesAsync();
            albumId = album.Id;
        }

        var yaSuscrito = await _context.UsuariosAlbumes
            .AnyAsync(ua => ua.UsuarioId == usuarioId && ua.AlbumId == albumId.Value);

        if (!yaSuscrito)
        {
            _context.UsuariosAlbumes.Add(new UsuarioAlbum
            {
                UsuarioId = usuarioId,
                AlbumId = albumId.Value
            });
        }

        await _context.SaveChangesAsync();

        return Ok(new { albumId = albumId.Value, nombre = nombreAlbum });
    }

    [HttpPost]
    public async Task<IActionResult> Crear([FromBody] JugadorDto dto)
    {
        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del jugador es obligatorio.");

        var equipos = await ValidarEquipos(dto.Equipos);
        if (equipos is null)
            return BadRequest("Uno o más equipos especificados no existen.");

        var jugador = new Jugador
        {
            Nombre = dto.Nombre.Trim(),
            NombreCompleto = dto.NombreCompleto?.Trim() ?? string.Empty,
            Equipos = equipos
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
            .FirstOrDefaultAsync(j => j.Id == id);

        if (existente is null)
            return NotFound();

        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del jugador es obligatorio.");

        var equipos = await ValidarEquipos(dto.Equipos);
        if (equipos is null)
            return BadRequest("Uno o más equipos especificados no existen.");

        existente.Nombre = dto.Nombre.Trim();
        existente.NombreCompleto = dto.NombreCompleto?.Trim() ?? string.Empty;
        existente.Equipos = equipos;

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
                Equipos = j.Equipos
                    .OrderBy(e => e.Nombre)
                    .Select(e => new
                    {
                        e.Id,
                        e.Nombre
                    })
                    .ToList()
            })
            .Cast<object?>()
            .FirstOrDefaultAsync();
    }

    private async Task<List<Equipo>?> ValidarEquipos(IReadOnlyCollection<long>? equipoIds)
    {
        if (equipoIds is null || equipoIds.Count == 0)
            return null;

        var ids = equipoIds.Distinct().ToList();
        var equipos = await _context.Equipos
            .Where(e => ids.Contains(e.Id))
            .ToListAsync();

        if (equipos.Count != ids.Count)
            return null;

        return equipos;
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
    public List<long> Equipos { get; set; } = new();
}
