using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CromosList.Migrations
{
    /// <inheritdoc />
    public partial class AllowCromoWithoutEquipo : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_cromo_equipo_equipo_id",
                table: "cromo");

            migrationBuilder.AlterColumn<long>(
                name: "equipo_id",
                table: "cromo",
                type: "bigint",
                nullable: true,
                oldClrType: typeof(long),
                oldType: "bigint");

            migrationBuilder.AddForeignKey(
                name: "FK_cromo_equipo_equipo_id",
                table: "cromo",
                column: "equipo_id",
                principalTable: "equipo",
                principalColumn: "id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_cromo_equipo_equipo_id",
                table: "cromo");

            migrationBuilder.AlterColumn<long>(
                name: "equipo_id",
                table: "cromo",
                type: "bigint",
                nullable: false,
                defaultValue: 0L,
                oldClrType: typeof(long),
                oldType: "bigint",
                oldNullable: true);

            migrationBuilder.AddForeignKey(
                name: "FK_cromo_equipo_equipo_id",
                table: "cromo",
                column: "equipo_id",
                principalTable: "equipo",
                principalColumn: "id",
                onDelete: ReferentialAction.Cascade);
        }
    }
}
