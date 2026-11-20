namespace CromosList.Models;

public class Equipo
    {
        public long Id { get; set; }
        public string Nombre { get; set; } = string.Empty;
        public string? TransfermarktId { get; set; }
        public ICollection<JugadorEquipo> JugadorEquipos { get; set; } = new List<JugadorEquipo>();
        public ICollection<Cromo> Cromos { get; set; } = new List<Cromo>();
    }
