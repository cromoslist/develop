namespace CromosList.Models;

public class Jugador
{
    public long Id { get; set; }
    public string Nombre { get; set; } = string.Empty;
    public string NombreCompleto { get; set; } = string.Empty;
    public bool Internacional { get; set; }
    public ICollection<Cromo> Cromos { get; set; } = new List<Cromo>();

    // Etapas del jugador en cada equipo (equipo + temporada inicio/fin).
    public ICollection<JugadorEquipo> Equipos { get; set; } = new List<JugadorEquipo>();

    // Años en los que fue internacional.
    public ICollection<JugadorAnioInternacional> AniosInternacional { get; set; } = new List<JugadorAnioInternacional>();

    public ICollection<Pais> Paises { get; set; } = new List<Pais>();
}
