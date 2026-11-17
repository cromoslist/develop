using System.Globalization;
using System.Text;
using CromosList.Data;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using CromosList.Dtos;
using CromosList.Models;

namespace CromosList.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class CromoController : ControllerBase
{
    // Los cromos de un álbum personalizado solo se ven para su usuario.
    private const string TemporadaPersonalizado = "Personalizado";

    private readonly CromosListDbContext _context;

    public CromoController(CromosListDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll([FromQuery] string? q)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var consulta = _context.Cromos
            .AsNoTracking()
            .Include(c => c.Jugador)
            .Include(c => c.Equipo)
            .Include(c => c.Edicion)
            .Include(c => c.Publicacion)
            .Include(c => c.TipoCromo)
            .Include(c => c.Coleccion)
            .Include(c => c.Album)
            .AsQueryable();

        if (!esAdmin)
            consulta = OcultarAlbumesPersonalizados(consulta, miId);

        if (!string.IsNullOrWhiteSpace(q))
        {
            var termino = q.Trim();
            consulta = consulta.Where(c =>
                c.Jugador.Nombre.ToLower().Contains(termino.ToLower())
                || c.Jugador.NombreCompleto.ToLower().Contains(termino.ToLower())
                || (c.Equipo != null && c.Equipo.Nombre.ToLower().Contains(termino.ToLower())));
        }

        var cromos = await consulta
            .OrderBy(c => c.Jugador.Nombre)
            .Select(c => new
            {
                c.Id,
                c.Numero,
                c.Comentario,

                Jugador = new
                {
                    c.Jugador.Id,
                    c.Jugador.Nombre,
                    c.Jugador.NombreCompleto
                },

                Equipo = c.Equipo == null
                    ? null
                    : new { c.Equipo.Id, c.Equipo.Nombre },

                Edicion = new
                {
                    c.Edicion.Id,
                    c.Edicion.Nombre
                },

                Publicacion = c.Publicacion == null
                    ? null
                    : new
                    {
                        c.Publicacion.Id,
                        c.Publicacion.Temporada,
                        c.Publicacion.Nombre,
                        c.Publicacion.FechaPublicacion
                    },

                TipoCromo = new
                {
                    c.TipoCromo.Id,
                    c.TipoCromo.Nombre
                },

                Coleccion = c.Coleccion == null
                    ? null
                    : new
                    {
                        c.Coleccion.Id,
                        c.Coleccion.Nombre
                    },

                Album = new
                {
                    c.Album.Id,
                    c.Album.Nombre,
                    c.Album.Temporada
                }
            })
            .ToListAsync();

        return Ok(cromos);
    }

    [HttpGet("{id:long}")]
    public async Task<IActionResult> GetById(long id)
    {
        var (miId, esAdmin) = await MiUsuarioAsync();

        var consulta = _context.Cromos
            .AsNoTracking()
            .Include(c => c.Jugador)
            .Include(c => c.Equipo)
            .Include(c => c.Edicion)
            .Include(c => c.Publicacion)
            .Include(c => c.TipoCromo)
            .Include(c => c.Coleccion)
            .Include(c => c.Album)
            .Where(c => c.Id == id);

        if (!esAdmin)
            consulta = OcultarAlbumesPersonalizados(consulta, miId);

        var cromo = await consulta
            .Select(c => new
            {
                c.Id,
                c.Numero,
                c.Comentario,

                Jugador = new
                {
                    c.Jugador.Id,
                    c.Jugador.Nombre,
                    c.Jugador.NombreCompleto
                },

                Equipo = c.Equipo == null
                    ? null
                    : new { c.Equipo.Id, c.Equipo.Nombre },

                Edicion = new
                {
                    c.Edicion.Id,
                    c.Edicion.Nombre
                },

                Publicacion = c.Publicacion == null
                    ? null
                    : new
                    {
                        c.Publicacion.Id,
                        c.Publicacion.Temporada,
                        c.Publicacion.Nombre,
                        c.Publicacion.FechaPublicacion
                    },

                TipoCromo = new
                {
                    c.TipoCromo.Id,
                    c.TipoCromo.Nombre
                },

                Coleccion = c.Coleccion == null
                    ? null
                    : new
                    {
                        c.Coleccion.Id,
                        c.Coleccion.Nombre
                    },

                Album = new
                {
                    c.Album.Id,
                    c.Album.Nombre,
                    c.Album.Temporada
                }
            })
            .FirstOrDefaultAsync();

        if (cromo is null)
            return NotFound();

        return Ok(cromo);
    }

    [HttpPost]
    public async Task<IActionResult> Create(CreateCromoDto dto)
    {
        if (!dto.EquipoId.HasValue)
            return BadRequest("El equipo es obligatorio para crear un cromo manualmente.");

        var jugadorExiste = await _context.Jugadores
            .AnyAsync(j => j.Id == dto.JugadorId);

        if (!jugadorExiste)
            return BadRequest("El jugador no existe.");

        var equipoExiste = !dto.EquipoId.HasValue || await _context.Equipos
            .AnyAsync(e => e.Id == dto.EquipoId.Value);

        if (!equipoExiste)
            return BadRequest("El equipo no existe.");

        var edicionExiste = await _context.Ediciones
            .AnyAsync(e => e.Id == dto.EdicionId);

        if (!edicionExiste)
            return BadRequest("La edición no existe.");

        var publicacionExiste = await _context.Publicaciones
            .AnyAsync(p => p.Id == dto.PublicacionId);

        if (dto.PublicacionId.HasValue && !publicacionExiste)
            return BadRequest("La publicación no existe.");

        var tipoCromoExiste = await _context.TiposCromo
            .AnyAsync(t => t.Id == dto.TipoCromoId);

        if (!tipoCromoExiste)
            return BadRequest("El tipo de cromo no existe.");

        var coleccionExiste = await _context.Colecciones
            .AnyAsync(c => c.Id == dto.ColeccionId);

        if (dto.ColeccionId.HasValue && !coleccionExiste)
            return BadRequest("La colección no existe.");

        var albumExiste = await _context.Albums
            .AnyAsync(a => a.Id == dto.AlbumId);

        if (!albumExiste)
            return BadRequest("El álbum no existe.");

        var cromo = new Models.Cromo
        {
            Numero = dto.Numero,
            JugadorId = dto.JugadorId,
            EquipoId = dto.EquipoId,
            EdicionId = dto.EdicionId,
            PublicacionId = dto.PublicacionId,
            TipoCromoId = dto.TipoCromoId,
            ColeccionId = dto.ColeccionId,
            AlbumId = dto.AlbumId,
            Comentario = dto.Comentario
        };

        _context.Cromos.Add(cromo);

        await _context.SaveChangesAsync();

        return CreatedAtAction(
            nameof(GetById),
            new { id = cromo.Id },
            cromo);
    }

    [HttpPost("importar-lote")]
    public async Task<IActionResult> ImportarLote(
        [FromBody] ImportarCromosLoteDto dto,
        CancellationToken cancellationToken)
    {
        var (_, esAdmin) = await MiUsuarioAsync();
        if (!esAdmin)
            return Forbid();

        if (dto.Cromos is null || dto.Cromos.Count == 0)
            return BadRequest(new { message = "El archivo no contiene cromos para importar." });

        if (dto.Cromos.Count > 1000)
            return BadRequest(new { message = "Se pueden importar como máximo 1000 cromos por archivo." });

        var albumValido = await _context.Albums
            .AnyAsync(a => a.Id == dto.AlbumId, cancellationToken);
        if (!albumValido)
            return BadRequest(new { message = "El álbum seleccionado no existe." });

        var errores = new List<object>();
        var filasRepetidas = dto.Cromos
            .Where(c => !string.IsNullOrWhiteSpace(c.Numero))
            .GroupBy(c => ClaveCromo(c.EquipoId, c.TipoCromoId, c.EdicionId, c.Numero))
            .Where(g => g.Count() > 1)
            .SelectMany(g => g)
            .Select(c => c.FilaCsv)
            .ToHashSet();

        foreach (var (cromo, indice) in dto.Cromos.Select((c, i) => (c, i)))
        {
            if (string.IsNullOrWhiteSpace(cromo.Numero))
                errores.Add(new { filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2, mensaje = "El número está vacío." });
            else if (cromo.Numero.Trim().Length > 100)
                errores.Add(new { filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2, mensaje = "El número supera los 100 caracteres." });

            if (filasRepetidas.Contains(cromo.FilaCsv))
                errores.Add(new { filaCsv = cromo.FilaCsv, mensaje = $"El número «{cromo.Numero.Trim()}» está repetido para el mismo equipo, tipo y edición en el CSV." });

            if ((cromo.EquipoId.HasValue && cromo.EquipoId.Value <= 0)
                || (!cromo.EquipoId.HasValue && !cromo.PermitirSinEquipo)
                || cromo.TipoCromoId <= 0 || cromo.EdicionId <= 0)
                errores.Add(new { filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2, mensaje = "Equipo, tipo de cromo o edición no válidos." });

            if ((!cromo.JugadorId.HasValue || cromo.JugadorId.Value <= 0)
                && !cromo.EsEscudo && !cromo.PermitirSinEquipo)
                errores.Add(new { filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2, mensaje = "El jugador no es válido." });

            if (cromo.PermitirSinEquipo && string.IsNullOrWhiteSpace(cromo.NombreJugador))
                errores.Add(new { filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2, mensaje = "El nombre del jugador es obligatorio para permitir cromos sin equipo." });
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var equiposIds = dto.Cromos
            .Where(c => c.EquipoId.HasValue)
            .Select(c => c.EquipoId!.Value)
            .Distinct()
            .ToList();
        var tiposIds = dto.Cromos.Select(c => c.TipoCromoId).Distinct().ToList();
        var edicionesIds = dto.Cromos.Select(c => c.EdicionId).Distinct().ToList();
        var equiposDelAlbum = await _context.AlbumEquipos
            .Where(ae => ae.AlbumId == dto.AlbumId && equiposIds.Contains(ae.EquipoId))
            .Select(ae => ae.EquipoId)
            .Distinct()
            .ToListAsync(cancellationToken);
        var tiposDelAlbum = await _context.AlbumTiposCromo
            .Where(at => at.AlbumId == dto.AlbumId && tiposIds.Contains(at.TipoCromoId))
            .Select(at => at.TipoCromoId)
            .Distinct()
            .ToListAsync(cancellationToken);
        var edicionesValidas = await _context.Ediciones
            .Where(e => edicionesIds.Contains(e.Id))
            .Select(e => e.Id)
            .ToListAsync(cancellationToken);
        var equiposValidos = equiposDelAlbum.ToHashSet();
        var tiposValidos = tiposDelAlbum.ToHashSet();
        var edicionesValidasSet = edicionesValidas.ToHashSet();

        foreach (var (cromo, indice) in dto.Cromos.Select((c, i) => (c, i)))
        {
            var fila = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2;
            if (cromo.EquipoId.HasValue && !equiposValidos.Contains(cromo.EquipoId.Value))
                errores.Add(new { filaCsv = fila, mensaje = "El equipo no pertenece al álbum seleccionado." });
            if (!tiposValidos.Contains(cromo.TipoCromoId))
                errores.Add(new { filaCsv = fila, mensaje = "El tipo de cromo no pertenece al álbum seleccionado." });
            if (!edicionesValidasSet.Contains(cromo.EdicionId))
                errores.Add(new { filaCsv = fila, mensaje = "La edición seleccionada no existe." });
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var filasJugadorSinEquipo = dto.Cromos
            .Where(c => c.PermitirSinEquipo)
            .ToList();
        var jugadoresExistentes = filasJugadorSinEquipo.Count == 0
            ? new List<Jugador>()
            : await _context.Jugadores
                .AsNoTracking()
                .ToListAsync(cancellationToken);
        var jugadoresNuevosPorNombre = new Dictionary<string, Jugador>(StringComparer.Ordinal);
        var jugadoresNuevosPorFila = new Dictionary<ImportarCromoFilaDto, Jugador>();

        foreach (var fila in filasJugadorSinEquipo)
        {
            if (fila.JugadorId.HasValue)
            {
                if (!jugadoresExistentes.Any(j => j.Id == fila.JugadorId.Value))
                    errores.Add(new { filaCsv = fila.FilaCsv, mensaje = "El jugador indicado no existe." });
                continue;
            }

            var nombre = fila.NombreJugador.Trim();
            var nombreNormalizado = NormalizarNombre(nombre);
            var coincidencias = jugadoresExistentes
                .Where(j => NormalizarNombre(j.Nombre) == nombreNormalizado
                    || NormalizarNombre(j.NombreCompleto) == nombreNormalizado)
                .GroupBy(j => j.Id)
                .Select(g => g.First())
                .ToList();

            if (coincidencias.Count == 1)
            {
                fila.JugadorId = coincidencias[0].Id;
                continue;
            }

            if (!jugadoresNuevosPorNombre.TryGetValue(nombreNormalizado, out var jugadorNuevo))
            {
                jugadorNuevo = new Jugador
                {
                    Nombre = nombre,
                    NombreCompleto = nombre,
                    Internacional = false
                };
                jugadoresNuevosPorNombre[nombreNormalizado] = jugadorNuevo;
                _context.Jugadores.Add(jugadorNuevo);
            }

            jugadoresNuevosPorFila[fila] = jugadorNuevo;
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var escudosSolicitados = dto.Cromos
            .Where(c => c.EsEscudo && !c.PermitirSinEquipo)
            .ToList();
        var jugadoresEnEquipos = await _context.JugadorEquipos
            .Where(je => equiposIds.Contains(je.EquipoId))
            .Select(je => new
            {
                je.EquipoId,
                je.JugadorId,
                je.Jugador.Nombre,
                je.Jugador.NombreCompleto
            })
            .Distinct()
            .ToListAsync(cancellationToken);

        var escudosNuevosNecesarios = new HashSet<long>();
        foreach (var (cromo, indice) in escudosSolicitados.Select((c, i) => (c, i)))
        {
            var candidatos = jugadoresEnEquipos
                .Where(j => j.EquipoId == cromo.EquipoId!.Value
                    && (j.Nombre.Contains("escudo", StringComparison.OrdinalIgnoreCase)
                        || j.NombreCompleto.Contains("escudo", StringComparison.OrdinalIgnoreCase)))
                .Select(j => j.JugadorId)
                .Distinct()
                .ToList();

            if (candidatos.Count > 1)
            {
                errores.Add(new
                {
                    filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2,
                    mensaje = "Hay más de un registro especial de Escudo asociado a este equipo."
                });
            }
            else if (candidatos.Count == 1)
            {
                cromo.JugadorId = candidatos[0];
            }
            else
            {
                escudosNuevosNecesarios.Add(cromo.EquipoId!.Value);
            }
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var jugadorIds = dto.Cromos
            .Where(c => !c.EsEscudo && !c.PermitirSinEquipo)
            .Select(c => c.JugadorId!.Value)
            .Distinct()
            .ToList();
        var jugadoresDelEquipo = await _context.JugadorEquipos
            .Where(je => equiposIds.Contains(je.EquipoId) && jugadorIds.Contains(je.JugadorId))
            .Select(je => new { je.EquipoId, je.JugadorId })
            .Distinct()
            .ToListAsync(cancellationToken);
        var jugadoresValidos = jugadoresDelEquipo
            .Select(je => $"{je.EquipoId}:{je.JugadorId}")
            .ToHashSet(StringComparer.Ordinal);

        foreach (var (cromo, indice) in dto.Cromos.Select((c, i) => (c, i)))
        {
            if (!cromo.EsEscudo && !cromo.PermitirSinEquipo
                && !jugadoresValidos.Contains($"{cromo.EquipoId!.Value}:{cromo.JugadorId!.Value}"))
                errores.Add(new
                {
                    filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2,
                    mensaje = "El jugador no pertenece al equipo indicado en esta fila."
                });
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var incluirCromosSinEquipo = dto.Cromos.Any(c => !c.EquipoId.HasValue);
        var cromosExistentes = await _context.Cromos
            .AsNoTracking()
            .Where(c => c.AlbumId == dto.AlbumId
                && ((c.EquipoId.HasValue && equiposIds.Contains(c.EquipoId.Value))
                    || (!c.EquipoId.HasValue && incluirCromosSinEquipo))
                && tiposIds.Contains(c.TipoCromoId)
                && edicionesIds.Contains(c.EdicionId))
            .Select(c => new { c.EquipoId, c.TipoCromoId, c.EdicionId, c.Numero })
            .ToListAsync(cancellationToken);
        var clavesExistentes = cromosExistentes
            .Select(c => ClaveCromo(c.EquipoId, c.TipoCromoId, c.EdicionId, c.Numero))
            .ToHashSet(StringComparer.Ordinal);

        foreach (var (cromo, indice) in dto.Cromos.Select((c, i) => (c, i)))
        {
            if (clavesExistentes.Contains(ClaveCromo(cromo.EquipoId, cromo.TipoCromoId, cromo.EdicionId, cromo.Numero)))
                errores.Add(new
                {
                    filaCsv = cromo.FilaCsv > 0 ? cromo.FilaCsv : indice + 2,
                    mensaje = $"El número «{cromo.Numero.Trim()}» ya existe en este álbum, equipo, tipo y edición."
                });
        }

        if (errores.Count > 0)
            return BadRequest(new { message = "Corrige los errores del CSV antes de importar.", errores });

        var escudosCreados = new Dictionary<long, Jugador>();
        if (escudosNuevosNecesarios.Count > 0)
        {
            var equipos = await _context.Equipos
                .Where(e => escudosNuevosNecesarios.Contains(e.Id))
                .Select(e => new { e.Id, e.Nombre })
                .ToListAsync(cancellationToken);

            foreach (var equipo in equipos)
            {
                var nombreEscudo = $"Escudo {equipo.Nombre}";
                var jugador = new Jugador
                {
                    Nombre = nombreEscudo,
                    NombreCompleto = nombreEscudo,
                    Internacional = false
                };
                jugador.Equipos.Add(new JugadorEquipo
                {
                    Jugador = jugador,
                    EquipoId = equipo.Id,
                    TemporadaInicio = "Actualidad",
                    TemporadaFin = string.Empty
                });

                _context.Jugadores.Add(jugador);
                escudosCreados[equipo.Id] = jugador;
            }
        }

        var cromos = new List<Models.Cromo>();
        foreach (var fila in dto.Cromos)
        {
            var cromo = new Models.Cromo
            {
                Numero = fila.Numero.Trim(),
                AlbumId = dto.AlbumId,
                EquipoId = fila.EquipoId,
                TipoCromoId = fila.TipoCromoId,
                EdicionId = fila.EdicionId,
                JugadorId = fila.JugadorId.GetValueOrDefault(),
                Comentario = string.IsNullOrWhiteSpace(fila.Comentario) ? null : fila.Comentario.Trim(),
                PublicacionId = null,
                ColeccionId = null
            };

            if (jugadoresNuevosPorFila.TryGetValue(fila, out var jugadorNuevo))
                cromo.Jugador = jugadorNuevo;

            if (fila.EsEscudo && fila.JugadorId.GetValueOrDefault() <= 0
                && fila.EquipoId.HasValue
                && escudosCreados.TryGetValue(fila.EquipoId.Value, out var escudo))
                cromo.Jugador = escudo;

            cromos.Add(cromo);
        }

        _context.Cromos.AddRange(cromos);
        await _context.SaveChangesAsync(cancellationToken);

        return Ok(new
        {
            creados = cromos.Count,
            escudosCreados = escudosCreados.Count,
            jugadoresCreados = jugadoresNuevosPorNombre.Count
        });
    }

    private static string ClaveCromo(long? equipoId, long tipoCromoId, long edicionId, string numero)
    {
        return $"{equipoId?.ToString() ?? "sin-equipo"}:{tipoCromoId}:{edicionId}:{numero.Trim().ToUpperInvariant()}";
    }

    private static string NormalizarNombre(string nombre)
    {
        var descompuesto = nombre.Trim().ToLowerInvariant().Normalize(NormalizationForm.FormD);
        var sinTildes = new string(descompuesto
            .Where(caracter => CharUnicodeInfo.GetUnicodeCategory(caracter) != UnicodeCategory.NonSpacingMark)
            .ToArray())
            .Normalize(NormalizationForm.FormC);
        return string.Join(' ', sinTildes.Split(' ', StringSplitOptions.RemoveEmptyEntries));
    }

    [HttpPut("{id:long}")]
    public async Task<IActionResult> Editar(long id, [FromBody] CreateCromoDto dto)
    {
        var cromo = await _context.Cromos.FindAsync(id);

        if (cromo is null)
            return NotFound();

        var jugadorExiste = await _context.Jugadores
            .AnyAsync(j => j.Id == dto.JugadorId);

        if (!jugadorExiste)
            return BadRequest("El jugador no existe.");

        var equipoExiste = !dto.EquipoId.HasValue || await _context.Equipos
            .AnyAsync(e => e.Id == dto.EquipoId.Value);

        if (!equipoExiste)
            return BadRequest("El equipo no existe.");

        var edicionExiste = await _context.Ediciones
            .AnyAsync(e => e.Id == dto.EdicionId);

        if (!edicionExiste)
            return BadRequest("La edición no existe.");

        var publicacionExiste = await _context.Publicaciones
            .AnyAsync(p => p.Id == dto.PublicacionId);

        if (dto.PublicacionId.HasValue && !publicacionExiste)
            return BadRequest("La publicación no existe.");

        var tipoCromoExiste = await _context.TiposCromo
            .AnyAsync(t => t.Id == dto.TipoCromoId);

        if (!tipoCromoExiste)
            return BadRequest("El tipo de cromo no existe.");

        var coleccionExiste = await _context.Colecciones
            .AnyAsync(c => c.Id == dto.ColeccionId);

        if (dto.ColeccionId.HasValue && !coleccionExiste)
            return BadRequest("La colección no existe.");

        var albumExiste = await _context.Albums
            .AnyAsync(a => a.Id == dto.AlbumId);

        if (!albumExiste)
            return BadRequest("El álbum no existe.");

        cromo.Numero = dto.Numero;
        cromo.JugadorId = dto.JugadorId;
        cromo.EquipoId = dto.EquipoId;
        cromo.EdicionId = dto.EdicionId;
        cromo.PublicacionId = dto.PublicacionId;
        cromo.TipoCromoId = dto.TipoCromoId;
        cromo.ColeccionId = dto.ColeccionId;
        cromo.AlbumId = dto.AlbumId;

        // El comentario es opcional: si no viene en la peticion se conserva
        // el valor actual para no perderlo al editar desde otras pantallas.
        if (dto.Comentario is not null)
            cromo.Comentario = dto.Comentario;

        await _context.SaveChangesAsync();

        return Ok(cromo);
    }

    [HttpDelete("{id:long}")]
    public async Task<IActionResult> Eliminar(long id)
    {
        var cromo = await _context.Cromos
            .Include(c => c.UsuariosCromo)
            .FirstOrDefaultAsync(c => c.Id == id);

        if (cromo is null)
            return NotFound();

        _context.UsuariosCromo.RemoveRange(cromo.UsuariosCromo);

        _context.Cromos.Remove(cromo);

        await _context.SaveChangesAsync();

        return NoContent();
    }

    // Descarta los cromos que pertenecen a los álbumes personalizados de otros
    // usuarios, dejando solo los del propio usuario.
    private IQueryable<Models.Cromo> OcultarAlbumesPersonalizados(
        IQueryable<Models.Cromo> consulta,
        long usuarioId)
    {
        // Sin id de usuario no se puede comprobar la pertenencia, asi que se
        // ocultan todos los cromos de álbumes personalizados.
        var miosAlbumes = usuarioId == 0
            ? Enumerable.Empty<long>().AsQueryable()
            : _context.UsuariosAlbumes
                .Where(ua => ua.UsuarioId == usuarioId)
                .Select(ua => ua.AlbumId);

        return consulta.Where(c =>
            c.Album.Temporada != TemporadaPersonalizado ||
            miosAlbumes.Contains(c.AlbumId));
    }

    private long ObtenerMiId()
    {
        var sub = User.FindFirst(System.Security.Claims.ClaimTypes.NameIdentifier)?.Value
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
