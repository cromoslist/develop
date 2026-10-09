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
import { Jugador, EquipoDeJugador, JugadorService } from '../../services/jugador.service';
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
  columnas = ['nombre', 'nombreCompleto', 'equipos', 'temporadas', 'acciones'];

  busqueda = new FormControl('');
  temporadaFiltro = new FormControl('', { nonNullable: true });

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

    return this.jugadores.filter(j => {
      const coincideTexto =
        !filtro ||
        normalizarTexto(j.nombre).includes(filtro) ||
        normalizarTexto(j.nombreCompleto).includes(filtro);

      return coincideTexto && this.cumpleTemporada(j);
    });
  }

  get hayFiltro(): boolean {
    return this.filtro.length > 0 || !!this.temporadaFiltro.value;
  }

  get sinResultados(): boolean {
    return !this.cargando && !this.error && this.jugadoresVisibles.length === 0;
  }

  get temporadasDisponibles(): string[] {
    const valores = new Set<string>();

    for (const jugador of this.jugadores) {
      for (const rango of this.rangosDeJugador(jugador)) {
        const inicio = this.anioDe(rango.temporadaInicio);
        const fin = this.anioDe(rango.temporadaFin) ?? inicio;

        if (inicio !== null && fin !== null && fin >= inicio) {
          for (let anio = inicio; anio <= fin; anio++) {
            valores.add(this.etiquetaTemporada(anio));
          }
        } else {
          if (rango.temporadaInicio) {
            valores.add(rango.temporadaInicio);
          }

          if (rango.temporadaFin) {
            valores.add(rango.temporadaFin);
          }
        }
      }
    }

    return [...valores].sort((a, b) => {
      const anioA = this.anioDe(a) ?? 0;
      const anioB = this.anioDe(b) ?? 0;
      return anioA - anioB || a.localeCompare(b);
    });
  }

  limpiarBusqueda(): void {
    this.busqueda.setValue('');
  }

  equiposDe(jugador: Jugador): string {
    return jugador.equipos.map(e => e.nombre).join(', ');
  }

  temporadasDe(jugador: Jugador): string {
    return this.rangosDeJugador(jugador)
      .map(rango => {
        const inicio = rango.temporadaInicio || '?';
        return rango.temporadaFin
          ? `${inicio} - ${rango.temporadaFin}`
          : `${inicio} - actualidad`;
      })
      .join(', ');
  }

  aniosInternacional(jugador: Jugador): string {
    return (jugador.aniosInternacional ?? []).join(', ');
  }

  private rangosDeJugador(jugador: Jugador): EquipoDeJugador[] {
    return jugador.equipos.filter(e => e.id === this.equipoId);
  }

  private cumpleTemporada(jugador: Jugador): boolean {
    const seleccion = this.temporadaFiltro.value;

    if (!seleccion) {
      return true;
    }

    return this.rangosDeJugador(jugador).some(rango => this.cumpleRango(rango, seleccion));
  }

  private cumpleRango(rango: EquipoDeJugador, seleccion: string): boolean {
    const objetivo = this.anioDe(seleccion);
    const inicio = this.anioDe(rango.temporadaInicio);

    if (objetivo === null || inicio === null) {
      return rango.temporadaInicio === seleccion || rango.temporadaFin === seleccion;
    }

    if (objetivo < inicio) {
      return false;
    }

    if (!rango.temporadaFin) {
      return true;
    }

    const fin = this.anioDe(rango.temporadaFin);
    return fin === null ? rango.temporadaFin === seleccion : objetivo <= fin;
  }

  private anioDe(temporada: string): number | null {
    const coincidencia = /(\d{4})/.exec(temporada ?? '');
    return coincidencia ? Number(coincidencia[1]) : null;
  }

  private etiquetaTemporada(anio: number): string {
    return `${anio}/${anio + 1}`;
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