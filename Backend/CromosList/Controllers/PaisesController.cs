using CromosList.Data;
using CromosList.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class PaisesController : ControllerBase
{
    private readonly CromosListDbContext _context;

    public PaisesController(CromosListDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var paises = await _context.Paises
            .AsNoTracking()
            .OrderBy(p => p.Nombre)
            .ToListAsync();

        return Ok(paises);
    }

    [HttpPost]
    public async Task<IActionResult> Crear([FromBody] PaisDto dto)
    {
        var nombre = dto.Nombre?.Trim();

        if (string.IsNullOrWhiteSpace(nombre))
        {
            return BadRequest("El nombre del país es obligatorio.");
        }

        var existente = await _context.Paises
            .FirstOrDefaultAsync(p => p.Nombre.ToLower() == nombre.ToLower());

        if (existente is not null)
        {
            return Ok(existente);
        }

        var pais = new Pais { Nombre = nombre };
        _context.Paises.Add(pais);
        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetAll), new { id = pais.Id }, pais);
    }
}

public class PaisDto
{
    public string? Nombre { get; set; }
}
