using System.Linq.Expressions;
using CromosList.Data;
using CromosList.Models;
using Microsoft.EntityFrameworkCore;

namespace CromosList.Services;

// Lógica común de los álbumes personalizados: un álbum nuevo e independiente
// (temporada "Personalizado") copiado de los cromos que cumplan el filtro
// indicado (por jugador, por equipo, etc.), suscrito al usuario autenticado.
public class AlbumPersonalizadoService
{
    // Marca de los álbumes creados por un usuario para sus jugadores o equipos.
    public const string TemporadaAlbumPersonalizado = "Personalizado";

    private readonly CromosListDbContext _context;

    public AlbumPersonalizadoService(CromosListDbContext context)
    {
        _context = context;
    }

    // Devuelve el id del álbum (creado o reutilizado) o un mensaje de error.
    public async Task<(long AlbumId, string? Error)> CrearAsync(
        long usuarioId,
        string nombreAlbum,
        string sujeto,
        Expression<Func<Cromo, bool>> filtroCromos)
    {
        var cromos = await _context.Cromos
            .AsNoTracking()
            .Where(filtroCromos)
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
            return (0, $"El {sujeto} no tiene cromos todavía.");

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

        return (albumId.Value, null);
    }
}
