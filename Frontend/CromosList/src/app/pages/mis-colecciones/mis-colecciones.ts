import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { AlbumService, Album } from '../../services/album.service';
import { UsuarioAlbumService, ColeccionSuscripcion } from '../../services/usuario-album.service';
import { JugadorService, Jugador } from '../../services/jugador.service';
import { EquipoService, Equipo } from '../../services/equipo.service';
import { AuthService } from '../../services/auth.service';

export interface ColeccionItem {
  albumId: number;
  nombre: string;
  temporada: string;
  editorial: { id: number; nombre: string };
  totalCromos: number;
  suscrito: boolean;
  tenidos: number;
  faltan: number;
  completado: boolean;
}

@Component({
  selector: 'app-mis-colecciones',
  imports: [
    CommonModule,
    RouterLink,
    ReactiveFormsModule,
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatFormFieldModule,
    MatInputModule,
    MatAutocompleteModule
  ],
  templateUrl: './mis-colecciones.html',
  styleUrl: './mis-colecciones.scss'
})
export class MisColeccionesComponent implements OnInit {

  colecciones: ColeccionItem[] = [];
  cargando = true;
  cambiando = false;
  error = '';

  jugadores: Jugador[] = [];
  jugadorControl = new FormControl<string | Jugador>('');
  jugadoresFiltrados: Jugador[] = [];

  equipos: Equipo[] = [];
  equipoControl = new FormControl<string | Equipo>('');
  equiposFiltrados: Equipo[] = [];

  anadiendo = false;
  errorPersonalizado = '';

  private miId: number | null = null;

  constructor(
    private albumService: AlbumService,
    private usuarioAlbumService: UsuarioAlbumService,
    private jugadorService: JugadorService,
    private equipoService: EquipoService,
    private authService: AuthService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.miId = this.authService.getMiId();
    if (!this.miId) {
      this.error = 'No se ha podido identificar al usuario.';
      this.cargando = false;
      return;
    }
    this.cargar();
    this.cargarJugadores();
    this.cargarEquipos();
    this.jugadorControl.valueChanges.subscribe(value => this.filtrarJugadores(value));
    this.equipoControl.valueChanges.subscribe(value => this.filtrarEquipos(value));
  }

  get tieneSuscripciones(): boolean {
    return this.colecciones.some(c => c.suscrito);
  }

  trackAlbum(_: number, item: ColeccionItem): number {
    return item.albumId;
  }

  trackJugador(_: number, jugador: Jugador): number {
    return jugador.id;
  }

  trackEquipo(_: number, equipo: Equipo): number {
    return equipo.id;
  }

  displayJugador(jugador?: Jugador | null): string {
    return jugador ? jugador.nombre : '';
  }

  displayEquipo(equipo?: Equipo | null): string {
    return equipo ? equipo.nombre : '';
  }

  private filtrarJugadores(valor: string | Jugador | null): void {
    const texto = typeof valor === 'string' ? (valor || '').trim().toLowerCase() : '';
    this.jugadoresFiltrados = texto
      ? this.jugadores.filter(j => (j.nombre || '').toLowerCase().includes(texto))
      : this.jugadores.slice(0, 50);
    this.cdr.detectChanges();
  }

  private filtrarEquipos(valor: string | Equipo | null): void {
    const texto = typeof valor === 'string' ? (valor || '').trim().toLowerCase() : '';
    this.equiposFiltrados = texto
      ? this.equipos.filter(e => (e.nombre || '').toLowerCase().includes(texto))
      : this.equipos.slice(0, 50);
    this.cdr.detectChanges();
  }

  anadirJugador(): void {
    if (this.anadiendo) return;

    const valor = this.jugadorControl.value;
    const jugador = typeof valor === 'object' && valor ? valor : null;

    if (!jugador) {
      this.errorPersonalizado = 'Selecciona un jugador de la lista.';
      this.cdr.detectChanges();
      return;
    }

    this.errorPersonalizado = '';
    this.anadiendo = true;

    this.jugadorService.crearAlbumPersonalizado(jugador.id).subscribe({
      next: () => {
        this.anadiendo = false;
        this.jugadorControl.reset();
        this.cargar();
      },
      error: () => {
        this.anadiendo = false;
        this.errorPersonalizado = 'No se ha podido crear el álbum del jugador.';
        this.cdr.detectChanges();
      }
    });
  }

  anadirEquipo(): void {
    if (this.anadiendo) return;

    const valor = this.equipoControl.value;
    const equipo = typeof valor === 'object' && valor ? valor : null;

    if (!equipo) {
      this.errorPersonalizado = 'Selecciona un equipo de la lista.';
      this.cdr.detectChanges();
      return;
    }

    this.errorPersonalizado = '';
    this.anadiendo = true;

    this.equipoService.crearAlbumPersonalizado(equipo.id).subscribe({
      next: () => {
        this.anadiendo = false;
        this.equipoControl.reset();
        this.cargar();
      },
      error: () => {
        this.anadiendo = false;
        this.errorPersonalizado = 'No se ha podido crear el álbum del equipo.';
        this.cdr.detectChanges();
      }
    });
  }

  private cargarJugadores(): void {
    this.jugadorService.getJugadores().subscribe({
      next: data => {
        this.jugadores = data;
        this.jugadoresFiltrados = data.slice(0, 50);
        this.cdr.detectChanges();
      },
      error: () => {
        this.errorPersonalizado = 'No se han podido cargar los jugadores.';
        this.cdr.detectChanges();
      }
    });
  }

  private cargarEquipos(): void {
    this.equipoService.getEquipos().subscribe({
      next: data => {
        this.equipos = data;
        this.equiposFiltrados = data.slice(0, 50);
        this.cdr.detectChanges();
      },
      error: () => {
        this.errorPersonalizado = 'No se han podido cargar los equipos.';
        this.cdr.detectChanges();
      }
    });
  }

  progreso(item: ColeccionItem): number {
    if (!item.totalCromos) return 0;
    return Math.round((item.tenidos / item.totalCromos) * 100);
  }

  private cargar(): void {
    this.cargando = true;
    this.error = '';
    const usuarioId = this.miId!;

    this.albumService.getAlbumes().subscribe({
      next: albumes => {
        this.usuarioAlbumService.listarSuscripciones(usuarioId).subscribe({
          next: suscripciones => {
            this.colecciones = this.combinar(albumes, suscripciones);
            this.cargando = false;
            this.cdr.detectChanges();
          },
          error: () => {
            this.cargando = false;
            this.error = 'No se han podido cargar tus colecciones.';
            this.cdr.detectChanges();
          }
        });
      },
      error: () => {
        this.cargando = false;
        this.error = 'No se han podido cargar las colecciones.';
        this.cdr.detectChanges();
      }
    });
  }

  private combinar(albumes: Album[], suscripciones: ColeccionSuscripcion[]): ColeccionItem[] {
    const suscritos = new Map(suscripciones.map(s => [s.albumId, s]));

    return albumes.map(a => {
      const s = suscritos.get(a.id);
      return {
        albumId: a.id,
        nombre: a.nombre,
        temporada: a.temporada,
        editorial: a.editorial,
        totalCromos: s?.totalCromos ?? a.contadorCromos,
        suscrito: !!s,
        tenidos: s?.tenidos ?? 0,
        faltan: s?.faltan ?? a.contadorCromos,
        completado: s?.completado ?? false
      };
    }).sort((a, b) => (a.suscrito === b.suscrito ? a.nombre.localeCompare(b.nombre) : a.suscrito ? -1 : 1));
  }

  apuntarse(item: ColeccionItem): void {
    if (!this.miId || this.cambiando) return;
    this.cambiando = true;
    this.error = '';

    this.usuarioAlbumService.suscribir({ usuarioId: this.miId, albumId: item.albumId }).subscribe({
      next: () => {
        this.cambiando = false;
        this.cargar();
      },
      error: () => {
        this.cambiando = false;
        this.error = 'No se ha podido apuntar a la colección.';
        this.cdr.detectChanges();
      }
    });
  }

  desapuntarse(item: ColeccionItem): void {
    if (!this.miId || this.cambiando) return;
    this.cambiando = true;
    this.error = '';

    this.usuarioAlbumService.desuscribir(this.miId, item.albumId).subscribe({
      next: () => {
        this.cambiando = false;
        this.cargar();
      },
      error: () => {
        this.cambiando = false;
        this.error = 'No se ha podido desapuntar de la colección.';
        this.cdr.detectChanges();
      }
    });
  }

  esPersonalizado(item: ColeccionItem): boolean {
    return item.temporada === 'Personalizado';
  }

  eliminarPersonalizado(item: ColeccionItem): void {
    if (this.cambiando) return;

    const confirmado = confirm(
      `¿Seguro que quieres eliminar "${item.nombre}"?\n\n` +
      'Se borrará de la base de datos junto con todos sus cromos y las marcas que hayas hecho. ' +
      'Esta acción no se puede deshacer.'
    );

    if (!confirmado) return;

    this.cambiando = true;
    this.error = '';

    this.albumService.eliminarAlbumPersonalizado(item.albumId).subscribe({
      next: () => {
        this.cambiando = false;
        this.cargar();
      },
      error: () => {
        this.cambiando = false;
        this.error = 'No se ha podido eliminar el álbum.';
        this.cdr.detectChanges();
      }
    });
  }
}