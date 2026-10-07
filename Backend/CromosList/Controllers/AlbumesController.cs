using System.Security.Claims;
using CromosList.Data;
using CromosList.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class AlbumesController : ControllerBase
{
    // Los álbumes personalizados son exclusivos del usuario que los creo.
    private const string TemporadaPersonalizado = "Personalizado";

    private readonly CromosListDbContext _context;

    public AlbumesController(CromosListDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var albums = await Proyeccion(null, miId, esAdmin)
            .ToListAsync();

        return Ok(albums);
    }

    [HttpGet("{id:long}")]
    public async Task<IActionResult> GetById(long id)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var album = await Proyeccion(id, miId, esAdmin)
            .FirstOrDefaultAsync();

        if (album is null)
            return NotFound();

        return Ok(album);
    }

    // Detalle "ver todo": incluye los cromos de cada equipo del álbum.
    [HttpGet("{id:long}/Cromos")]
    public async Task<IActionResult> GetCromosPorEquipo(long id)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var visible = await AlbumesVisibles(miId, esAdmin)
            .AnyAsync(a => a.Id == id);

        if (!visible)
            return NotFound();

        var titulo = await _context.Albums
            .AsNoTracking()
            .Where(a => a.Id == id)
            .Select(a => new { a.Id, a.Nombre, a.Temporada })
            .FirstOrDefaultAsync();

        if (titulo is null)
            return NotFound();

        var cromos = await _context.Cromos
            .AsNoTracking()
            .Include(c => c.Jugador)
            .Include(c => c.Equipo)
            .Include(c => c.TipoCromo)
            .Include(c => c.Edicion)
            .Include(c => c.Publicacion)
            .Include(c => c.Coleccion)
            .Where(c => c.AlbumId == id)
            .ToListAsync();

        // La temporada de cada colección se busca en el álbum de origen, que
        // se empareja por nombre. Si el cromo trae publicación, se usa la suya.
        var temporadasPorAlbum = await _context.Albums
            .AsNoTracking()
            .Where(a => a.Temporada != TemporadaPersonalizado)
            .Select(a => new { a.Nombre, a.Temporada })
            .ToListAsync();

        var temporadasPorNombre = new Dictionary<string, List<string>>();

        foreach (var album in temporadasPorAlbum)
        {
            if (!temporadasPorNombre.TryGetValue(album.Nombre, out var lista))
            {
                lista = new List<string>();
                temporadasPorNombre[album.Nombre] = lista;
            }

            if (!lista.Contains(album.Temporada))
                lista.Add(album.Temporada);
        }

        string TemporadaDe(Cromo cromo)
        {
            var dePublicacion = cromo.Publicacion?.Temporada?.Trim();
            if (!string.IsNullOrEmpty(dePublicacion))
                return dePublicacion;

            var nombreColeccion = cromo.Coleccion?.Nombre;
            if (nombreColeccion is null)
                return string.Empty;

            return temporadasPorNombre.TryGetValue(nombreColeccion, out var temporadas)
                ? string.Join(" · ", temporadas)
                : string.Empty;
        }

        var cromosPorEquipo = cromos
            .GroupBy(c => c.EquipoId)
            .ToDictionary(g => g.Key, g => g
                .Select(c => new
                {
                    c.Id,
                    c.Numero,
                    c.Comentario,
                    Jugador = new { c.Jugador.Id, c.Jugador.Nombre },
                    Equipo = new { c.Equipo.Id, c.Equipo.Nombre },
                    Edicion = new { c.Edicion.Id, c.Edicion.Nombre },
                    Temporada = TemporadaDe(c),
                    TipoCromo = new { c.TipoCromo.Id, c.TipoCromo.Nombre },
                    Coleccion = c.Coleccion == null
                        ? null
                        : new { c.Coleccion.Id, c.Coleccion.Nombre }
                })
                .OrderBy(c => c.Numero)
                .ToList());

        var equipos = await _context.AlbumEquipos
            .AsNoTracking()
            .Include(ae => ae.Equipo)
            .Where(ae => ae.AlbumId == id)
            .OrderBy(ae => ae.Orden)
            .Select(ae => new
            {
                ae.Equipo.Id,
                ae.Equipo.Nombre,
                ae.Orden
            })
            .ToListAsync();

        var tiposCromo = await _context.AlbumTiposCromo
            .AsNoTracking()
            .Include(at => at.TipoCromo)
            .Where(at => at.AlbumId == id)
            .OrderBy(at => at.Orden)
            .Select(at => new
            {
                at.TipoCromo.Id,
                at.TipoCromo.Nombre,
                at.Orden
            })
            .ToListAsync();

        var resultado = equipos.Select(e => new
        {
            e.Id,
            e.Nombre,
            e.Orden,
            Cromos = cromosPorEquipo.TryGetValue(e.Id, out var lista)
                ? lista.Cast<object>()
                : Enumerable.Empty<object>()
        });

        return Ok(new
        {
            titulo.Id,
            titulo.Nombre,
            titulo.Temporada,
            Equipos = resultado,
            TiposCromo = tiposCromo
        });
    }

    // Proyección plana y segura para la serialización (evita ciclos objeto).
    private IQueryable<object> Proyeccion(long? id, long usuarioId, bool esAdmin)
    {
        IQueryable<Album> consulta = AlbumesVisibles(usuarioId, esAdmin)
            .Include(a => a.Editorial)
            .Include(a => a.AlbumEquipos).ThenInclude(ae => ae.Equipo)
            .Include(a => a.AlbumTiposCromo).ThenInclude(at => at.TipoCromo)
            .Include(a => a.Cromos);

        if (id.HasValue)
            consulta = consulta.Where(a => a.Id == id.Value);

        return consulta
            .Select(a => new
            {
                a.Id,
                a.Nombre,
                a.Temporada,
                Editorial = new { a.Editorial.Id, a.Editorial.Nombre },
                Equipos = a.AlbumEquipos
                    .OrderBy(ae => ae.Orden)
                    .Select(ae => new { ae.Equipo.Id, ae.Equipo.Nombre, ae.Orden })
                    .ToList(),
                TiposCromo = a.AlbumTiposCromo
                    .OrderBy(at => at.Orden)
                    .Select(at => new { at.TipoCromo.Id, at.TipoCromo.Nombre, at.Orden })
                    .ToList(),
                ContadorCromos = a.Cromos.Count
            });
    }

    // Los álbumes personalizados solo se ven para su propio usuario (o un admin).
    private IQueryable<Album> AlbumesVisibles(long usuarioId, bool esAdmin)
    {
        var consulta = _context.Albums.AsNoTracking();

        if (esAdmin)
            return consulta;

        // Sin id de usuario no se puede probar la pertenencia: se ocultan todos
        // los personalizedalbums antes que exponerlos por error.
        var miosAlbumes = usuarioId == 0
            ? Enumerable.Empty<long>().AsQueryable()
            : _context.UsuariosAlbumes
                .Where(ua => ua.UsuarioId == usuarioId)
                .Select(ua => ua.AlbumId);

        return consulta.Where(a =>
            a.Temporada != TemporadaPersonalizado ||
            miosAlbumes.Contains(a.Id));
    }

    private async Task<bool> PuedeVerAlbum(long albumId, long usuarioId, bool esAdmin)
    {
        return await AlbumesVisibles(usuarioId, esAdmin)
            .AnyAsync(a => a.Id == albumId);
    }

    private async Task<(long UsuarioId, bool EsAdmin)> MiUsuarioAsync()
    {
        var id = ObtenerMiId();

        if (id == 0)
            return (0, false);

        var esAdmin = await _context.Usuarios
            .AsNoTracking()
            .AnyAsync(u => u.Id == id && u.EsAdmin);

        return (id, esAdmin);
    }

    [HttpPost]
    public async Task<IActionResult> Crear([FromBody] AlbumDto dto)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del álbum es obligatorio.");

        if (string.IsNullOrWhiteSpace(dto.Temporada))
            return BadRequest("La temporada es obligatoria.");

        if (!await _context.Editoriales.AnyAsync(e => e.Id == dto.EditorialId))
            return BadRequest("La editorial especificada no existe.");

        if (dto.Equipos is null || dto.Equipos.Count == 0)
            return BadRequest("Debe añadir al menos un equipo.");

        if (dto.TiposCromo is null || dto.TiposCromo.Count == 0)
            return BadRequest("Debe añadir al menos un tipo de cromo.");

        var album = new Album
        {
            Nombre = dto.Nombre,
            Temporada = dto.Temporada,
            EditorialId = dto.EditorialId
        };

        _context.Albums.Add(album);
        await _context.SaveChangesAsync();

        for (int i = 0; i < dto.Equipos.Count; i++)
        {
            var eq = dto.Equipos[i];
            if (!await _context.Equipos.AnyAsync(e => e.Id == eq.EquipoId))
                return BadRequest($"El equipo con id {eq.EquipoId} no existe.");

            _context.AlbumEquipos.Add(new AlbumEquipo
            {
                AlbumId = album.Id,
                EquipoId = eq.EquipoId,
                Orden = i
            });
        }

        for (int i = 0; i < dto.TiposCromo.Count; i++)
        {
            var tc = dto.TiposCromo[i];
            if (!await _context.TiposCromo.AnyAsync(t => t.Id == tc.TipoCromoId))
                return BadRequest($"El tipo de cromo con id {tc.TipoCromoId} no existe.");

            _context.AlbumTiposCromo.Add(new AlbumTipoCromo
            {
                AlbumId = album.Id,
                TipoCromoId = tc.TipoCromoId,
                Orden = i
            });
        }

        await _context.SaveChangesAsync();

        var nuevoAlbum = await Proyeccion(album.Id, miId, esAdmin).FirstOrDefaultAsync();

        return CreatedAtAction(nameof(GetById), new { id = album.Id }, nuevoAlbum);
    }

    [HttpPut("{id:long}")]
    public async Task<IActionResult> Editar(long id, [FromBody] AlbumDto dto)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var existente = await _context.Albums
            .Include(a => a.AlbumEquipos)
            .Include(a => a.AlbumTiposCromo)
            .FirstOrDefaultAsync(a => a.Id == id);

        if (existente is null)
            return NotFound();

        if (!await PuedeVerAlbum(id, miId, esAdmin))
            return Forbid();

        if (string.IsNullOrWhiteSpace(dto.Nombre))
            return BadRequest("El nombre del álbum es obligatorio.");

        if (string.IsNullOrWhiteSpace(dto.Temporada))
            return BadRequest("La temporada es obligatoria.");

        if (!await _context.Editoriales.AnyAsync(e => e.Id == dto.EditorialId))
            return BadRequest("La editorial especificada no existe.");

        if (dto.Equipos is null || dto.Equipos.Count == 0)
            return BadRequest("Debe añadir al menos un equipo.");

        if (dto.TiposCromo is null || dto.TiposCromo.Count == 0)
            return BadRequest("Debe añadir al menos un tipo de cromo.");

        existente.Nombre = dto.Nombre;
        existente.Temporada = dto.Temporada;
        existente.EditorialId = dto.EditorialId;

        _context.AlbumEquipos.RemoveRange(existente.AlbumEquipos);
        _context.AlbumTiposCromo.RemoveRange(existente.AlbumTiposCromo);

        for (int i = 0; i < dto.Equipos.Count; i++)
        {
            var eq = dto.Equipos[i];
            if (!await _context.Equipos.AnyAsync(e => e.Id == eq.EquipoId))
                return BadRequest($"El equipo con id {eq.EquipoId} no existe.");

            existente.AlbumEquipos.Add(new AlbumEquipo
            {
                AlbumId = id,
                EquipoId = eq.EquipoId,
                Orden = i
            });
        }

        for (int i = 0; i < dto.TiposCromo.Count; i++)
        {
            var tc = dto.TiposCromo[i];
            if (!await _context.TiposCromo.AnyAsync(t => t.Id == tc.TipoCromoId))
                return BadRequest($"El tipo de cromo con id {tc.TipoCromoId} no existe.");

            existente.AlbumTiposCromo.Add(new AlbumTipoCromo
            {
                AlbumId = id,
                TipoCromoId = tc.TipoCromoId,
                Orden = i
            });
        }

        await _context.SaveChangesAsync();

        var albumActualizado = await Proyeccion(id, miId, esAdmin).FirstOrDefaultAsync();

        return Ok(albumActualizado);
    }

    [HttpDelete("{id:long}")]
    public async Task<IActionResult> Eliminar(long id)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var album = await _context.Albums.FindAsync(id);

        if (album is null)
            return NotFound();

        if (!await PuedeVerAlbum(id, miId, esAdmin))
            return Forbid();

        // Un album personalizado se borra siempre en cascada.
        if (album.Temporada == TemporadaPersonalizado)
        {
            await BorrarAlbumCompleto(_context, id, album);
            return NoContent();
        }

        var cromos = await _context.Cromos
            .Where(c => c.AlbumId == id)
            .ToListAsync();

        var cromoIds = cromos.Select(c => c.Id).ToList();

        if (cromoIds.Count > 0)
        {
            var cromosUsuario = await _context.UsuariosCromo
                .Where(uc => cromoIds.Contains(uc.CromoId))
                .ToListAsync();

            _context.UsuariosCromo.RemoveRange(cromosUsuario);
        }

        _context.Cromos.RemoveRange(cromos);

        var suscripcionesAlbum = await _context.UsuariosAlbumes
            .Where(ua => ua.AlbumId == id)
            .ToListAsync();

        _context.UsuariosAlbumes.RemoveRange(suscripcionesAlbum);

        var albumEquipos = await _context.AlbumEquipos
            .Where(ae => ae.AlbumId == id)
            .ToListAsync();

        _context.AlbumEquipos.RemoveRange(albumEquipos);

        var albumTipos = await _context.AlbumTiposCromo
            .Where(at => at.AlbumId == id)
            .ToListAsync();

        _context.AlbumTiposCromo.RemoveRange(albumTipos);

        _context.Albums.Remove(album);
        await _context.SaveChangesAsync();

        return NoContent();
    }

    // Elimina un álbum personalizado: se borra siempre de la base de datos,
    // junto con sus cromos, marcas de los usuarios y relaciones de equipos
    // y tipos de cromo. Los personalizedalbums no se comparten, asi que no
    // queda ningun suscriptor al que respetar.
    [HttpDelete("{id:long}/personalizado")]
    public async Task<IActionResult> EliminarAlbumPersonalizado(long id)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        if (miId == 0 && !esAdmin)
            return Unauthorized();

        var album = await _context.Albums
            .Where(a => a.Id == id && a.Temporada == TemporadaPersonalizado)
            .FirstOrDefaultAsync();

        if (album is null)
            return NotFound("El álbum personalizado no existe.");

        if (!esAdmin && !await PuedeVerAlbum(id, miId, esAdmin))
            return Forbid();

        await BorrarAlbumCompleto(_context, id, album);

        return NoContent();
    }

    // Borra el album y todo lo que depende de el: cromos, marcas de los
    // usuarios, suscripciones, relaciones de equipos y tipos de cromo y la
    // coleccion autogenerada si ningun otro cromo la sigue usando.
    internal static async Task BorrarAlbumCompleto(CromosListDbContext context, long id, Album album)
    {
        var cromos = await context.Cromos
            .Where(c => c.AlbumId == id)
            .ToListAsync();

        var coleccionIds = cromos
            .Where(c => c.ColeccionId != null)
            .Select(c => c.ColeccionId!.Value)
            .Distinct()
            .ToList();

        var cromoIds = cromos.Select(c => c.Id).ToList();

        if (cromoIds.Count > 0)
        {
            var cromosUsuario = await context.UsuariosCromo
                .Where(uc => cromoIds.Contains(uc.CromoId))
                .ToListAsync();

            context.UsuariosCromo.RemoveRange(cromosUsuario);
        }

        context.Cromos.RemoveRange(cromos);

        var suscripciones = await context.UsuariosAlbumes
            .Where(ua => ua.AlbumId == id)
            .ToListAsync();

        context.UsuariosAlbumes.RemoveRange(suscripciones);

        var equipos = await context.AlbumEquipos
            .Where(ae => ae.AlbumId == id)
            .ToListAsync();

        context.AlbumEquipos.RemoveRange(equipos);

        var tipos = await context.AlbumTiposCromo
            .Where(at => at.AlbumId == id)
            .ToListAsync();

        context.AlbumTiposCromo.RemoveRange(tipos);

        context.Albums.Remove(album);

        await context.SaveChangesAsync();

        // La coleccion se crea sola al generar el album personalizado, asi que
        // se elimina cuando el album era el unico que la usaba.
        foreach (var coleccionId in coleccionIds)
        {
            var enUso = await context.Cromos
                .AnyAsync(c => c.ColeccionId == coleccionId);

            if (!enUso)
            {
                var coleccion = await context.Colecciones
                    .FirstOrDefaultAsync(ce => ce.Id == coleccionId);

                if (coleccion is not null)
                {
                    context.Colecciones.Remove(coleccion);
                    await context.SaveChangesAsync();
                }
            }
        }
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                  ?? User.FindFirst("sub")?.Value;

        return long.TryParse(sub!, out var id) ? id : 0;
    }
}

public class AlbumDto
{
    public string Nombre { get; set; } = string.Empty;
    public string Temporada { get; set; } = string.Empty;
    public long EditorialId { get; set; }
    public List<AlbumEquipoDto> Equipos { get; set; } = new();
    public List<AlbumTipoCromoDto> TiposCromo { get; set; } = new();
}

public class AlbumEquipoDto
{
    public long EquipoId { get; set; }
}

public class AlbumTipoCromoDto
{
    public long TipoCromoId { get; set; }
}
