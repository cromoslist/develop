using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CromosList.Migrations
{
    /// <inheritdoc />
    public partial class AddTransfermarktIdToJugador : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "transfermarkt_id",
                table: "jugador",
                type: "character varying(32)",
                maxLength: 32,
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_jugador_transfermarkt_id",
                table: "jugador",
                column: "transfermarkt_id",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_jugador_transfermarkt_id",
                table: "jugador");

            migrationBuilder.DropColumn(
                name: "transfermarkt_id",
                table: "jugador");
        }
    }
}
