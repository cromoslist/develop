using System.Security.Claims;
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
public class EquiposController : ControllerBase
{
    private readonly CromosListDbContext _context;
    private readonly AlbumPersonalizadoService _albumPersonalizado;

    public EquiposController(CromosListDbContext context, AlbumPersonalizadoService albumPersonalizado)
    {
        _context = context;
        _albumPersonalizado = albumPersonalizado;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var equipos = await _context.Equipos
            .AsNoTracking()
            .ToListAsync();

        return Ok(equipos);
    }

    [HttpPost]
    public async Task<IActionResult> Crear([FromBody] Equipo equipo)
    {
        if (string.IsNullOrWhiteSpace(equipo.Nombre))
        {
            return BadRequest("El nombre del equipo es obligatorio.");
        }

        _context.Equipos.Add(equipo);
        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetAll), new { id = equipo.Id }, equipo);
    }

    [HttpPut("{id:long}")]
    public async Task<IActionResult> Editar(long id, [FromBody] Equipo equipo)
    {
        var existente = await _context.Equipos.FindAsync(id);

        if (existente is null)
            return NotFound();

        if (string.IsNullOrWhiteSpace(equipo.Nombre))
        {
            return BadRequest("El nombre del equipo es obligatorio.");
        }

        existente.Nombre = equipo.Nombre;
        await _context.SaveChangesAsync();

        return Ok(existente);
    }

    [HttpGet("{id:long}/Jugadores")]
    public async Task<IActionResult> GetJugadores(long id)
    {
        var equipoExiste = await _context.Equipos
            .AnyAsync(e => e.Id == id);

        if (!equipoExiste)
            return NotFound();

        var jugadores = await _context.Jugadores
            .AsNoTracking()
            .Where(j => j.Equipos.Any(je => je.EquipoId == id))
            .Select(j => new
            {
                j.Id,
                j.Nombre,
                j.NombreCompleto
            })
            .OrderBy(j => j.Nombre)
            .ToListAsync();

        return Ok(jugadores);
    }

    // Crea un álbum nuevo e independiente con todos los cromos del equipo
    // y apunta al usuario autenticado a esa colección.
    [HttpPost("{id:long}/album-personalizado")]
    public async Task<IActionResult> CrearAlbumPersonalizado(long id)
    {
        var usuarioId = ObtenerMiId();
        if (usuarioId == 0)
            return Unauthorized();

        var equipo = await _context.Equipos
            .AsNoTracking()
            .Where(e => e.Id == id)
            .Select(e => new { e.Id, e.Nombre })
            .FirstOrDefaultAsync();

        if (equipo is null)
            return NotFound("El equipo no existe.");

        var nombreAlbum = $"Álbum de {equipo.Nombre}";

        var (albumId, error) = await _albumPersonalizado.CrearAsync(
            usuarioId, nombreAlbum, "equipo", c => c.EquipoId == id);

        if (error is not null)
            return BadRequest(error);

        return Ok(new { albumId, nombre = nombreAlbum });
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                  ?? User.FindFirst("sub")?.Value;

        return long.TryParse(sub!, out var id) ? id : 0;
    }
}