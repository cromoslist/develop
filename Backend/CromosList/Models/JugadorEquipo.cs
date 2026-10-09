namespace CromosList.Models;

// Etapa de un jugador en un equipo, con la temporada en la que llega y en la
// que se va (formato "2004/2005"; la de fin puede quedar vacía si sigue).
public class JugadorEquipo
{
    public long JugadorId { get; set; }
    public Jugador Jugador { get; set; } = null!;

    public long EquipoId { get; set; }
    public Equipo Equipo { get; set; } = null!;

    public string TemporadaInicio { get; set; } = string.Empty;
    public string TemporadaFin { get; set; } = string.Empty;
}
