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
public class UsuariosAlbumesController : ControllerBase
{
    // Los álbumes personalizados solo pertenecen a su usuario.
    private const string TemporadaPersonalizado = "Personalizado";

    private readonly CromosListDbContext _context;

    public UsuariosAlbumesController(CromosListDbContext context)
    {
        _context = context;
    }

    // Colecciones (álbumes) a los que el usuario está apuntado, con su progreso.
    [HttpGet("{usuarioId:long}")]
    public async Task<IActionResult> GetByUsuario(long usuarioId)
    {
        if (!await _context.Usuarios.AnyAsync(u => u.Id == usuarioId))
            return NotFound("El usuario no existe.");

        var (miId, esAdmin) = await MiUsuarioAsync();

        var albumes = await _context.UsuariosAlbumes
            .AsNoTracking()
            .Include(ua => ua.Album)
                .ThenInclude(a => a.Editorial)
            .Where(ua => ua.UsuarioId == usuarioId)
            .Select(ua => new
            {
                ua.AlbumId,
                ua.Album.Nombre,
                ua.Album.Temporada,
                Editorial = new { ua.Album.Editorial.Id, ua.Album.Editorial.Nombre }
            })
            .OrderBy(x => x.Nombre)
            .ToListAsync();

        // Los álbumes personalizados de otro usuario no se listan aquí. Sin id de
        // usuario no se pueden resolver, así que tampoco se muestran.
        var visibles = albumes
            .Where(x => x.Temporada != TemporadaPersonalizado
                        || esAdmin
                        || (miId > 0 && usuarioId == miId))
            .ToList();

        var ids = visibles.Select(x => x.AlbumId).ToList();

        var totales = await _context.Cromos
            .AsNoTracking()
            .Where(c => ids.Contains(c.AlbumId))
            .GroupBy(c => c.AlbumId)
            .Select(g => new { AlbumId = g.Key, Total = g.Count() })
            .ToDictionaryAsync(g => g.AlbumId, g => g.Total);

        var tenidos = await _context.UsuariosCromo
            .AsNoTracking()
            .Where(uc => uc.UsuarioId == usuarioId && ids.Contains(uc.Cromo.AlbumId))
            .GroupBy(uc => uc.Cromo.AlbumId)
            .Select(g => new { AlbumId = g.Key, Total = g.Select(x => x.CromoId).Distinct().Count() })
            .ToDictionaryAsync(g => g.AlbumId, g => g.Total);

        var resultado = visibles.Select(x =>
        {
            var total = totales.GetValueOrDefault(x.AlbumId);
            var tiene = tenidos.GetValueOrDefault(x.AlbumId);
            return new
            {
                x.AlbumId,
                x.Nombre,
                x.Temporada,
                x.Editorial,
                TotalCromos = total,
                Tenidos = tiene,
                Faltan = total - tiene,
                Completado = total > 0 && tiene >= total
            };
        });

        return Ok(resultado);
    }

    // Detalle de progreso de una colección concreta de un usuario.
    [HttpGet("{usuarioId:long}/albumes/{albumId:long}")]
    public async Task<IActionResult> GetProgresoAlbum(long usuarioId, long albumId)
    {
        var album = await _context.Albums
            .AsNoTracking()
            .Where(a => a.Id == albumId)
            .Select(a => new { a.Id, a.Nombre, a.Temporada })
            .FirstOrDefaultAsync();

        if (album is null)
            return NotFound("La colección no existe.");

        var (miId, esAdmin) = await MiUsuarioAsync();

        if (album.Temporada == TemporadaPersonalizado && !esAdmin && usuarioId != miId)
            return NotFound("La colección no existe.");

        var suscrito = await _context.UsuariosAlbumes
            .AnyAsync(ua => ua.UsuarioId == usuarioId && ua.AlbumId == albumId);

        var totalCromos = await _context.Cromos
            .CountAsync(c => c.AlbumId == albumId);

        var cromosTenidos = await _context.UsuariosCromo
            .AsNoTracking()
            .Where(uc => uc.UsuarioId == usuarioId && uc.Cromo.AlbumId == albumId)
            .Select(uc => uc.CromoId)
            .ToListAsync();

        return Ok(new
        {
            album.Id,
            album.Nombre,
            album.Temporada,
            Suscrito = suscrito,
            TotalCromos = totalCromos,
            Tenidos = cromosTenidos.Count,
            Faltan = totalCromos - cromosTenidos.Count,
            CromosTenidos = cromosTenidos
        });
    }

    // El usuario se apunta a una colección (se le asigna su copia).
    [HttpPost]
    public async Task<IActionResult> Suscribir([FromBody] SuscribirAlbumDto dto)
    {
        if (!await _context.Usuarios.AnyAsync(u => u.Id == dto.UsuarioId))
            return BadRequest("El usuario no existe.");

        var album = await _context.Albums
            .AsNoTracking()
            .Where(a => a.Id == dto.AlbumId)
            .Select(a => new { a.Temporada })
            .FirstOrDefaultAsync();

        if (album is null)
            return BadRequest("La colección no existe.");

        // Los álbumes personalizados se crean desde un jugador y solo son
        // visibles para su propio usuario: no se pueden compartir.
        if (album.Temporada == TemporadaPersonalizado)
            return BadRequest("Los álbumes personalizados son exclusivos del usuario.");

        var yaExiste = await _context.UsuariosAlbumes
            .AnyAsync(ua => ua.UsuarioId == dto.UsuarioId && ua.AlbumId == dto.AlbumId);

        if (yaExiste)
            return Conflict("El usuario ya está apuntado a esta colección.");

        var usuarioAlbum = new UsuarioAlbum
        {
            UsuarioId = dto.UsuarioId,
            AlbumId = dto.AlbumId
        };

        _context.UsuariosAlbumes.Add(usuarioAlbum);
        await _context.SaveChangesAsync();

        return Ok(usuarioAlbum);
    }

    // El usuario se desapunta de una colección. Si es un álbum personalizado,
    // el álbum se borra de la base de datos con todos sus cromos.
    [HttpDelete("{usuarioId:long}/{albumId:long}")]
    public async Task<IActionResult> Desuscribir(long usuarioId, long albumId)
    {
        var usuarioAlbum = await _context.UsuariosAlbumes
            .FirstOrDefaultAsync(ua => ua.UsuarioId == usuarioId && ua.AlbumId == albumId);

        if (usuarioAlbum is null)
            return NotFound("El usuario no está apuntado a esta colección.");

        var album = await _context.Albums
            .FirstOrDefaultAsync(a => a.Id == albumId);

        if (album is not null && album.Temporada == TemporadaPersonalizado)
        {
            await AlbumesController.BorrarAlbumCompleto(_context, albumId, album);
            return NoContent();
        }

        _context.UsuariosAlbumes.Remove(usuarioAlbum);
        await _context.SaveChangesAsync();

        return NoContent();
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                  ?? User.FindFirst("sub")?.Value;

        return long.TryParse(sub!, out var id) ? id : 0;
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
}

public class SuscribirAlbumDto
{
    public long UsuarioId { get; set; }
    public long AlbumId { get; set; }
}