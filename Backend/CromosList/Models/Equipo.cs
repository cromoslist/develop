namespace CromosList.Models;

public class Equipo
    {
        public long Id { get; set; }
        public string Nombre { get; set; } = string.Empty;
        public ICollection<JugadorEquipo> JugadorEquipos { get; set; } = new List<JugadorEquipo>();
        public ICollection<Cromo> Cromos { get; set; } = new List<Cromo>();
    }
