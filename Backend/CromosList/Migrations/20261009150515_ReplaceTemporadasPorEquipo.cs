using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CromosList.Migrations
{
    /// <inheritdoc />
    public partial class ReplaceTemporadasPorEquipo : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "internacional",
                table: "jugador",
                type: "boolean",
                nullable: false,
                defaultValue: false);

            migrationBuilder.CreateTable(
                name: "jugador_anio_internacional",
                columns: table => new
                {
                    jugador_id = table.Column<long>(type: "bigint", nullable: false),
                    anio = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_jugador_anio_internacional", x => new { x.jugador_id, x.anio });
                    table.ForeignKey(
                        name: "FK_jugador_anio_internacional_jugador_jugador_id",
                        column: x => x.jugador_id,
                        principalTable: "jugador",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "jugador_equipo",
                columns: table => new
                {
                    jugador_id = table.Column<long>(type: "bigint", nullable: false),
                    equipo_id = table.Column<long>(type: "bigint", nullable: false),
                    temporada_inicio = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false),
                    temporada_fin = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_jugador_equipo", x => new { x.jugador_id, x.equipo_id, x.temporada_inicio });
                    table.ForeignKey(
                        name: "FK_jugador_equipo_equipo_equipo_id",
                        column: x => x.equipo_id,
                        principalTable: "equipo",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_jugador_equipo_jugador_jugador_id",
                        column: x => x.jugador_id,
                        principalTable: "jugador",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_jugador_equipo_equipo_id",
                table: "jugador_equipo",
                column: "equipo_id");

            // Migra los equipos y temporadas existentes a las etapas por equipo:
            // la temporada de inicio y la de fin son la mínima y la máxima que
            // tenía registradas el jugador (vacías si no tenía ninguna).
            migrationBuilder.Sql(
                @"INSERT INTO jugador_equipo (jugador_id, equipo_id, temporada_inicio, temporada_fin)
                  SELECT ej.jugador_id,
                         ej.equipo_id,
                         COALESCE(MIN(jt.temporada), ''),
                         COALESCE(MAX(jt.temporada), '')
                  FROM equipo_jugador ej
                  LEFT JOIN jugador_temporada jt ON jt.jugador_id = ej.jugador_id
                  GROUP BY ej.jugador_id, ej.equipo_id
                  ON CONFLICT DO NOTHING;");

            migrationBuilder.DropTable(
                name: "equipo_jugador");

            migrationBuilder.DropTable(
                name: "jugador_temporada");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "equipo_jugador",
                columns: table => new
                {
                    equipo_id = table.Column<long>(type: "bigint", nullable: false),
                    jugador_id = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_equipo_jugador", x => new { x.equipo_id, x.jugador_id });
                    table.ForeignKey(
                        name: "FK_equipo_jugador_equipo_equipo_id",
                        column: x => x.equipo_id,
                        principalTable: "equipo",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_equipo_jugador_jugador_jugador_id",
                        column: x => x.jugador_id,
                        principalTable: "jugador",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

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

            migrationBuilder.CreateIndex(
                name: "IX_equipo_jugador_jugador_id",
                table: "equipo_jugador",
                column: "jugador_id");

            migrationBuilder.Sql(
                @"INSERT INTO equipo_jugador (jugador_id, equipo_id)
                  SELECT DISTINCT jugador_id, equipo_id FROM jugador_equipo
                  ON CONFLICT DO NOTHING;");

            migrationBuilder.DropTable(
                name: "jugador_anio_internacional");

            migrationBuilder.DropTable(
                name: "jugador_equipo");

            migrationBuilder.DropColumn(
                name: "internacional",
                table: "jugador");
        }
    }
}
