namespace CromosList.Models;

// Año en el que el jugador fue internacional (solo si Internacional es true).
public class JugadorAnioInternacional
{
    public long JugadorId { get; set; }
    public Jugador Jugador { get; set; } = null!;

    public int Anio { get; set; }
}
