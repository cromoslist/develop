namespace CromosList.Models;

public class Pais
{
    public long Id { get; set; }
    public string Nombre { get; set; } = string.Empty;
    public ICollection<Jugador> Jugadores { get; set; } = new List<Jugador>();
}
