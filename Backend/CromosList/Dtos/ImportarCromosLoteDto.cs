namespace CromosList.Dtos;

public class ImportarCromosLoteDto
{
    public long AlbumId { get; set; }
    public List<ImportarCromoFilaDto> Cromos { get; set; } = new();
}

public class ImportarCromoFilaDto
{
    public int FilaCsv { get; set; }
    public string Numero { get; set; } = string.Empty;
    public long EquipoId { get; set; }
    public long TipoCromoId { get; set; }
    public long EdicionId { get; set; }
    public long? JugadorId { get; set; }
    public bool EsEscudo { get; set; }
    public string? Comentario { get; set; }
}
