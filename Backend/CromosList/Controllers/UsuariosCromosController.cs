using CromosList.Data;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using CromosList.Dtos;
using System.Security.Claims;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class UsuariosCromosController : ControllerBase
{
    private readonly CromosListDbContext _context;

    public UsuariosCromosController(CromosListDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var usuariosCromos = await _context.UsuariosCromo
            .AsNoTracking()
            .ToListAsync();

        return Ok(usuariosCromos);
    }

    [HttpGet("{usuarioId:long}")]
    public async Task<IActionResult> GetByUsuario(long usuarioId)
    {
        var usuarioExiste = await _context.Usuarios
            .AnyAsync(u => u.Id == usuarioId);

        if (!usuarioExiste)
            return NotFound("El usuario no existe.");

        var cromos = await _context.UsuariosCromo
            .AsNoTracking()
            .Where(uc => uc.UsuarioId == usuarioId)
            .Include(uc => uc.Cromo)
                .ThenInclude(c => c.Jugador)
            .Include(uc => uc.Cromo)
                .ThenInclude(c => c.Equipo)
            .Include(uc => uc.Cromo)
                .ThenInclude(c => c.Edicion)
            .Include(uc => uc.Cromo)
                .ThenInclude(c => c.TipoCromo)
            .Select(uc => new
            {
                uc.Cromo.Id,
                uc.Cromo.Numero,

                Jugador = new
                {
                    uc.Cromo.Jugador.Id,
                    uc.Cromo.Jugador.Nombre
                },

                Equipo = new
                {
                    uc.Cromo.Equipo.Id,
                    uc.Cromo.Equipo.Nombre
                },

                Edicion = new
                {
                    uc.Cromo.Edicion.Id,
                    uc.Cromo.Edicion.Nombre
                },

                TipoCromo = new
                {
                    uc.Cromo.TipoCromo.Id,
                    uc.Cromo.TipoCromo.Nombre
                }
            })
            .ToListAsync();

        return Ok(cromos);
    }

    [HttpPost]
    public async Task<IActionResult> AddCromo(CreateUsuarioCromoDto dto)
    {
        var usuarioExiste = await _context.Usuarios
            .AnyAsync(u => u.Id == dto.UsuarioId);

        if (!usuarioExiste)
            return BadRequest("El usuario no existe.");

        var cromoExiste = await _context.Cromos
            .AnyAsync(c => c.Id == dto.CromoId);

        if (!cromoExiste)
            return BadRequest("El cromo no existe.");

        var yaExiste = await _context.UsuariosCromo
            .AnyAsync(uc =>
                uc.UsuarioId == dto.UsuarioId &&
                uc.CromoId == dto.CromoId);

        if (yaExiste)
            return Conflict("El usuario ya tiene este cromo.");

        var usuarioCromo = new Models.UsuarioCromo
        {
            UsuarioId = dto.UsuarioId,
            CromoId = dto.CromoId
        };

        _context.UsuariosCromo.Add(usuarioCromo);

        await _context.SaveChangesAsync();

        return Ok(usuarioCromo);
    }

    [HttpDelete("{usuarioId:long}/{cromoId:long}")]
    public async Task<IActionResult> QuitarCromo(long usuarioId, long cromoId)
    {
        var usuarioCromo = await _context.UsuariosCromo
            .FirstOrDefaultAsync(uc => uc.UsuarioId == usuarioId && uc.CromoId == cromoId);

        if (usuarioCromo is null)
            return NotFound("El usuario no tiene este cromo.");

        _context.UsuariosCromo.Remove(usuarioCromo);
        await _context.SaveChangesAsync();

        return NoContent();
    }

    [HttpPut("album/{albumId:long}/faltas")]
    public async Task<IActionResult> ImportarFaltas(long albumId, [FromBody] ImportarFaltasDto dto)
    {
        var claimId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value
                      ?? User.FindFirst("sub")?.Value;

        if (!long.TryParse(claimId, out var usuarioId) || usuarioId <= 0)
            return Unauthorized();

        if (dto.CromosFaltantesIds is null || dto.CromosFaltantesIds.Count > 10000)
            return BadRequest("La lista de faltas no es válida.");

        var albumExiste = await _context.Albums
            .AsNoTracking()
            .AnyAsync(a => a.Id == albumId);

        if (!albumExiste)
            return NotFound("La colección no existe.");

        var suscrito = await _context.UsuariosAlbumes
            .AnyAsync(ua => ua.UsuarioId == usuarioId && ua.AlbumId == albumId);

        if (!suscrito)
            return Forbid();

        var cromosAlbum = await _context.Cromos
            .AsNoTracking()
            .Where(c => c.AlbumId == albumId)
            .Select(c => c.Id)
            .ToListAsync();

        var idsAlbum = cromosAlbum.ToHashSet();
        var idsFaltantes = dto.CromosFaltantesIds.ToHashSet();

        if (idsFaltantes.Any(id => !idsAlbum.Contains(id)))
            return BadRequest("La lista contiene cromos que no pertenecen a esta colección.");

        var idsConseguidos = idsAlbum.Except(idsFaltantes).ToHashSet();
        var marcasActuales = await _context.UsuariosCromo
            .Where(uc => uc.UsuarioId == usuarioId && idsAlbum.Contains(uc.CromoId))
            .ToListAsync();
        var marcasActualesIds = marcasActuales.Select(uc => uc.CromoId).ToHashSet();

        _context.UsuariosCromo.RemoveRange(
            marcasActuales.Where(uc => !idsConseguidos.Contains(uc.CromoId)));

        var marcasNuevas = idsConseguidos
            .Except(marcasActualesIds)
            .Select(cromoId => new Models.UsuarioCromo
            {
                UsuarioId = usuarioId,
                CromoId = cromoId
            });
        await _context.UsuariosCromo.AddRangeAsync(marcasNuevas);
        await _context.SaveChangesAsync();

        return NoContent();
    }
}

public class ImportarFaltasDto
{
    public List<long> CromosFaltantesIds { get; set; } = [];
}
