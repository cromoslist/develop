import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MATERIAL_IMPORTS } from '../../shared/material.imports';
import { normalizarTexto } from '../../shared/texto.util';
import { EquipoService } from '../../services/equipo.service';
import { Jugador, JugadorService } from '../../services/jugador.service';
import { JugadorEditarComponent } from '../jugadores/jugador-editar';

@Component({
  selector: 'app-equipo-jugadores',
  imports: [
    CommonModule,
    RouterLink,
    ReactiveFormsModule,
    MatDialogModule,
    MatTableModule,
    MatTooltipModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './equipo-jugadores.html',
  styleUrl: './equipo-jugadores.scss'
})
export class EquipoJugadoresComponent implements OnInit {

  jugadores: Jugador[] = [];
  columnas = ['nombre', 'nombreCompleto', 'equipos', 'acciones'];

  busqueda = new FormControl('');

  equipoNombre = '';

  cargando = true;
  error = '';

  private equipoId = 0;

  constructor(
    private route: ActivatedRoute,
    private equipoService: EquipoService,
    private jugadorService: JugadorService,
    private dialog: MatDialog,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.equipoId = Number(this.route.snapshot.paramMap.get('id'));

    if (!this.equipoId) {
      this.error = 'Equipo no válido.';
      this.cargando = false;
      return;
    }

    this.cargarNombreEquipo();
    this.cargarJugadores();
  }

  private cargarNombreEquipo(): void {
    this.equipoService
      .getEquipos()
      .subscribe({
        next: equipos => {
          this.equipoNombre = equipos.find(e => e.id === this.equipoId)?.nombre ?? '';
          this.cdr.detectChanges();
        },
        error: () => {
          this.error = 'No se han podido cargar los datos del equipo.';
        }
      });
  }

  private cargarJugadores(): void {
    this.jugadorService
      .getJugadoresDeEquipo(this.equipoId)
      .subscribe({
        next: jugadores => {
          this.jugadores = jugadores.filter(j => !this.esEscudo(j.nombre));
          this.cargando = false;
          this.cdr.detectChanges();
        },
        error: () => {
          this.cargando = false;
          this.error = 'No se han podido cargar los jugadores del equipo.';
        }
      });
  }

  private esEscudo(nombre: string): boolean {
    return normalizarTexto(nombre).includes('escudo');
  }

  private get filtro(): string {
    return normalizarTexto(this.busqueda.value);
  }

  get jugadoresVisibles(): Jugador[] {
    const filtro = this.filtro;

    if (!filtro) {
      return this.jugadores;
    }

    return this.jugadores.filter(j =>
      normalizarTexto(j.nombre).includes(filtro) ||
      normalizarTexto(j.nombreCompleto).includes(filtro)
    );
  }

  get hayFiltro(): boolean {
    return this.filtro.length > 0;
  }

  get sinResultados(): boolean {
    return !this.cargando && !this.error && this.jugadoresVisibles.length === 0;
  }

  limpiarBusqueda(): void {
    this.busqueda.setValue('');
  }

  equiposDe(jugador: Jugador): string {
    return jugador.equipos.map(e => e.nombre).join(', ');
  }

  editarJugador(jugador: Jugador): void {
    const dialogRef = this.dialog.open(JugadorEditarComponent, {
      width: '520px',
      data: { jugador }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.cargarJugadores();
      }
    });
  }
}