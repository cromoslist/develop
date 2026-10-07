import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import {
  AbstractControl,
  FormBuilder,
  FormGroup,
  ValidationErrors,
  Validators,
  ReactiveFormsModule
} from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Observable, map, startWith } from 'rxjs';
import { MATERIAL_IMPORTS } from '../../shared/material.imports';
import { Album, AlbumEquipo, AlbumService, AlbumTipoCromo, AlbumCromo } from '../../services/album.service';
import { Edicion, EdicionService } from '../../services/edicion.service';
import { JugadorEquipo, JugadorService } from '../../services/jugador.service';
import { CromoEditarComponent } from '../coleccion-detalle/cromo-editar';

@Component({
  selector: 'app-cromos',
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatDialogModule,
    MatTableModule,
    MatTooltipModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './cromos.html',
  styleUrl: './cromos.scss'
})
export class AnadirCromoComponent implements OnInit {

  cromoForm!: FormGroup;

  albumes: Album[] = [];
  albumSeleccionado: Album | null = null;
  tiposCromoAlbum: AlbumTipoCromo[] = [];
  equiposAlbum: AlbumEquipo[] = [];
  ediciones: Edicion[] = [];
  jugadores: JugadorEquipo[] = [];

  filteredEquipos$!: Observable<AlbumEquipo[]>;
  filteredJugadores$!: Observable<JugadorEquipo[]>;

  cromos: AlbumCromo[] = [];
  columnas = ['numero', 'jugador', 'tipoCromo', 'edicion', 'acciones'];
  mostrarTabla = false;

  cargando = false;
  guardando = false;
  mensaje = '';
  error = '';

  private apiUrl = '/api';

  constructor(
    private fb: FormBuilder,
    private http: HttpClient,
    private albumService: AlbumService,
    private edicionService: EdicionService,
    private jugadorService: JugadorService,
    private dialog: MatDialog,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.crearFormulario();
    this.cargarDatos();
  }

  private crearFormulario(): void {
    this.cromoForm = this.fb.group({
      albumId: [
        null,
        Validators.required
      ],
      tipoCromoId: [
        null,
        Validators.required
      ],
      equipo: [
        null,
        [Validators.required, this.seleccionValida]
      ],
      numero: [
        '',
        Validators.required
      ],
      jugador: [
        null,
        [Validators.required, this.seleccionValida]
      ],
      edicionId: [
        null,
        Validators.required
      ],
      comentario: [
        ''
      ]
    });

    this.filteredEquipos$ = this.cromoForm.get('equipo')!.valueChanges.pipe(
      startWith(''),
      map(valor => this.filtrarEquipos(valor))
    );

    this.filteredJugadores$ = this.cromoForm.get('jugador')!.valueChanges.pipe(
      startWith(''),
      map(valor => this.filtrarJugadores(valor))
    );

    this.cromoForm.get('albumId')!.valueChanges.subscribe(id => {
      this.albumSeleccionado = this.albumes.find(a => a.id === id) ?? null;
      this.tiposCromoAlbum = this.albumSeleccionado?.tiposCromo ?? [];
      this.equiposAlbum = this.albumSeleccionado?.equipos ?? [];

      this.jugadores = [];
      this.cromos = [];
      this.mostrarTabla = false;

      this.cromoForm.patchValue({
        tipoCromoId: null,
        equipo: null,
        numero: '',
        jugador: null,
        edicionId: null,
        comentario: ''
      });

      this.cdr.detectChanges();
    });
  }

  private cargarDatos(): void {
    this.albumService
      .getAlbumes()
      .subscribe({
        next: albumes => {
          this.albumes = albumes;
          this.cdr.detectChanges();
        },
        error: () => {
          this.error = 'No se han podido cargar los álbumes.';
        }
      });

    this.edicionService
      .getEdiciones()
      .subscribe({
        next: ediciones => {
          this.ediciones = ediciones;
          this.cdr.detectChanges();
        },
        error: () => {
          this.error = 'No se han podido cargar las ediciones.';
        }
      });
  }

  // Los autocompletes guardan el objeto: si el usuario escribe texto y no elige
  // opcion, el valor es un string y no serviria para enviar el id.
  private seleccionValida(control: AbstractControl): ValidationErrors | null {
    const valor = control.value;
    return valor && typeof valor === 'object' ? null : { seleccionIncompleta: true };
  }

  get equipoId(): number | null {
    const equipo = this.cromoForm?.get('equipo')?.value;
    return equipo && typeof equipo === 'object' ? equipo.id : null;
  }

  filtrarEquipos(valor: any): AlbumEquipo[] {
    const filtro = (typeof valor === 'string' ? valor : '').toLowerCase();
    return this.equiposAlbum.filter(e => e.nombre.toLowerCase().includes(filtro));
  }

  filtrarJugadores(valor: any): JugadorEquipo[] {
    const filtro = (typeof valor === 'string' ? valor : '').toLowerCase();
    return this.jugadores.filter(j => (j.nombreCompleto || j.nombre).toLowerCase().includes(filtro));
  }

  displayEquipo(equipo: AlbumEquipo | null): string {
    return equipo?.nombre ?? '';
  }

  displayJugador(jugador: JugadorEquipo | null): string {
    if (!jugador) return '';
    return typeof jugador === 'string' ? jugador : (jugador.nombreCompleto || jugador.nombre);
  }

  onEquipoSelected(equipo: AlbumEquipo): void {
    this.cromoForm.get('equipo')?.setValue(equipo);
    this.onEquipoChange();
  }

  onEquipoChange(): void {
    this.jugadores = [];
    this.cromoForm.get('jugador')?.setValue(null);

    const equipoId = this.equipoId;

    if (!equipoId) return;

    this.jugadorService
      .getJugadoresPorEquipo(equipoId)
      .subscribe({
        next: jugadores => {
          this.jugadores = jugadores;
          this.cdr.detectChanges();
        },
        error: () => {
          this.error = 'No se han podido cargar los jugadores del equipo.';
        }
      });
  }

  guardar(): void {
    if (this.cromoForm.invalid) {
      this.cromoForm.markAllAsTouched();
      return;
    }

    this.guardando = true;
    this.mensaje = '';
    this.error = '';

    const payload = {
      numero: this.cromoForm.get('numero')?.value,
      albumId: this.albumSeleccionado!.id,
      equipoId: this.equipoId,
      jugadorId: this.cromoForm.get('jugador')?.value?.id,
      tipoCromoId: this.cromoForm.get('tipoCromoId')?.value,
      edicionId: this.cromoForm.get('edicionId')?.value,
      comentario: (this.cromoForm.get('comentario')?.value ?? '').trim() || null,
      publicacionId: null,
      coleccionId: null
    };

    this.http
      .post(`${this.apiUrl}/Cromo`, payload)
      .subscribe({
        next: () => {
          this.guardando = false;
          this.mensaje = 'Cromo añadido correctamente.';
          // Solo se limpian numero y jugador; album, tipo, equipo y edicion se
          // conservan para facilitar altas consecutivas del mismo equipo.
          this.cromoForm.patchValue({ numero: '', jugador: null });
          if (this.mostrarTabla) {
            this.verEquipo();
          }
          this.cdr.detectChanges();
        },
        error: () => {
          this.guardando = false;
          this.error = 'No se ha podido añadir el cromo.';
        }
      });
  }

  verEquipo(): void {
    const albumId = this.albumSeleccionado?.id;
    const equipoId = this.equipoId;

    if (!albumId || !equipoId) {
      this.error = 'Selecciona un álbum y un equipo para ver sus cromos.';
      return;
    }

    this.cargando = true;
    this.error = '';

    this.albumService
      .getAlbumConCromos(albumId)
      .subscribe({
        next: detalle => {
          this.cromos = detalle.equipos.find(e => e.id === equipoId)?.cromos ?? [];
          this.mostrarTabla = true;
          this.cargando = false;
          this.cdr.detectChanges();
        },
        error: () => {
          this.cargando = false;
          this.error = 'No se han podido cargar los cromos del equipo.';
        }
      });
  }

  editarCromo(cromo: AlbumCromo): void {
    if (!this.albumSeleccionado) return;

    const dialogRef = this.dialog.open(CromoEditarComponent, {
      width: '520px',
      data: { cromo, albumId: this.albumSeleccionado.id }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.verEquipo();
      }
    });
  }

  eliminarCromo(cromo: AlbumCromo): void {
    if (!confirm(`¿Seguro que quieres eliminar el cromo ${cromo.numero} de ${cromo.jugador.nombre}?`)) {
      return;
    }

    this.http
      .delete<void>(`${this.apiUrl}/Cromo/${cromo.id}`)
      .subscribe({
        next: () => this.verEquipo(),
        error: () => {
          this.error = 'No se ha podido eliminar el cromo.';
        }
      });
  }
}