using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CromosList.Migrations
{
    /// <inheritdoc />
    public partial class AddJugadorTemporada : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "jugador_temporada",
                columns: table => new
                {
                    jugador_id = table.Column<long>(type: "bigint", nullable: false),
                    temporada = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_jugador_temporada", x => new { x.jugador_id, x.temporada });
                    table.ForeignKey(
                        name: "FK_jugador_temporada_jugador_jugador_id",
                        column: x => x.jugador_id,
                        principalTable: "jugador",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            // Semilla: las temporadas que los jugadores ya tenían a través de
            // sus cromos, sin contar los álbumes personalizados de los usuarios.
            migrationBuilder.Sql(@"
                INSERT INTO jugador_temporada (jugador_id, temporada)
                SELECT DISTINCT c.jugador_id, a.temporada
                FROM cromo c
                INNER JOIN album a ON a.id = c.album_id
                WHERE a.temporada <> 'Personalizado'
                  AND a.temporada <> ''
                ON CONFLICT DO NOTHING;
            ");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "jugador_temporada");
        }
    }
}
