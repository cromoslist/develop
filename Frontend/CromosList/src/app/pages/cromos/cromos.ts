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
import { Observable, forkJoin, firstValueFrom, map, startWith } from 'rxjs';
import { MATERIAL_IMPORTS } from '../../shared/material.imports';
import { Album, AlbumEquipo, AlbumService, AlbumTipoCromo, AlbumCromo } from '../../services/album.service';
import { Edicion, EdicionService } from '../../services/edicion.service';
import { JugadorEquipo, JugadorService } from '../../services/jugador.service';
import { CromoEditarComponent } from '../coleccion-detalle/cromo-editar';
import { normalizarTexto } from '../../shared/texto.util';

interface FilaCromoCsv {
  filaCsv: number;
  numero: string;
  nombreJugador: string;
  equipo: string;
  equipoId: number | null;
  temporada: string;
  tipoCromo: string;
  tipoCromoId: number | null;
  edicion: string;
  edicionId: number | null;
  comentario: string | null;
  jugadorId: number | null;
  jugadorNombreValidado: string | null;
  jugadoresSugeridos: JugadorEquipo[];
  esEscudo: boolean;
  error: string;
}

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
  edicionesCargadas = false;
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
  cargandoJugadores = false;
  csvArchivo = '';
  csvFilas: FilaCromoCsv[] = [];
  csvMensaje = '';
  csvError = '';
  importandoCsv = false;
  cargandoCsv = false;

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
      this.limpiarCsv();

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
          this.edicionesCargadas = true;
          this.cdr.detectChanges();
        },
        error: () => {
          this.edicionesCargadas = true;
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

  get contextoCsvCompleto(): boolean {
    return !!this.albumSeleccionado && this.edicionesCargadas;
  }

  get csvImportable(): boolean {
    return this.contextoCsvCompleto
      && !this.csvError
      && !this.importandoCsv
      && !this.cargandoCsv
      && this.csvFilas.length > 0
      && this.csvFilas.length <= 500
      && this.csvFilas.every(fila =>
        !fila.error
        && (fila.jugadorId !== null || fila.esEscudo)
        && fila.equipoId !== null
        && fila.tipoCromoId !== null
        && fila.edicionId !== null
      );
  }

  get csvFilasVisibles(): FilaCromoCsv[] {
    return this.csvFilas.slice(0, 20);
  }

  get hayErroresCsv(): boolean {
    return this.csvFilas.some(fila =>
      !!fila.error || (fila.jugadorId === null && !fila.esEscudo)
    );
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

    if (!equipoId) {
      this.cargandoJugadores = false;
      return;
    }

    this.cargandoJugadores = true;
    this.jugadorService
      .getJugadoresPorEquipo(equipoId)
      .subscribe({
        next: jugadores => {
          this.jugadores = jugadores;
          this.cargandoJugadores = false;
          this.cdr.detectChanges();
        },
        error: () => {
          this.cargandoJugadores = false;
          this.error = 'No se han podido cargar los jugadores del equipo.';
          this.cdr.detectChanges();
        }
      });
  }

  async onCsvSelected(event: Event): Promise<void> {
    const input = event.target as HTMLInputElement;
    const archivo = input.files?.[0];
    input.value = '';

    this.limpiarCsv();
    if (!archivo) return;

    if (!this.contextoCsvCompleto) {
      this.csvError = 'Selecciona un álbum y espera a que se carguen sus datos antes de subir el CSV.';
      return;
    }

    if (archivo.size > 1_000_000) {
      this.csvError = 'El archivo supera el límite de 1 MB.';
      return;
    }

    this.csvArchivo = archivo.name;

    try {
      const texto = await archivo.text();
      const registros = this.parsearCsv(texto);
      if (registros.length < 2) {
        throw new Error('El CSV debe tener una cabecera y al menos una fila de datos.');
      }

      const cabeceras = registros[0].map(cabecera =>
        normalizarTexto(cabecera).replace(/[\s_-]/g, '')
      );
      const indiceNumero = this.buscarColumna(cabeceras, ['numero']);
      const indiceJugador = this.buscarColumna(cabeceras, ['nombrejugador', 'jugador', 'nombre']);
      const indiceEquipo = this.buscarColumna(cabeceras, ['equipo', 'nombreequipo', 'equipoid']);
      const indiceTemporada = this.buscarColumna(cabeceras, ['temporada', 'temporadainicio']);
      const indiceTipoCromo = this.buscarColumna(cabeceras, ['tipocromo', 'tipo', 'tipocromoid']);
      const indiceEdicion = this.buscarColumna(cabeceras, ['edicion', 'edition', 'edicionid']);
      const indiceComentario = this.buscarColumna(cabeceras, ['comentario', 'nota']);

      if ([indiceNumero, indiceJugador, indiceEquipo, indiceTemporada, indiceTipoCromo, indiceEdicion].some(indice => indice < 0)) {
        throw new Error('El CSV necesita las columnas «numero», «nombreJugador», «equipo», «temporada», «tipoCromo» y «edicion».');
      }

      const datos = registros.slice(1).filter(fila => fila.some(valor => valor.trim() !== ''));
      if (datos.length > 500) {
        throw new Error('El archivo supera el máximo de 500 cromos por importación.');
      }

      this.csvFilas = datos.map((fila, indice) => {
        const numero = (fila[indiceNumero] ?? '').trim();
        const nombreJugador = (fila[indiceJugador] ?? '').trim();
        const equipo = (fila[indiceEquipo] ?? '').trim();
        const temporada = (fila[indiceTemporada] ?? '').trim();
        const tipoCromo = (fila[indiceTipoCromo] ?? '').trim();
        const edicion = (fila[indiceEdicion] ?? '').trim();
        const comentario = indiceComentario >= 0
          ? (fila[indiceComentario] ?? '').trim() || null
          : null;

        const equipoResuelto = this.resolverElementoCsv(
          equipo,
          this.equiposAlbum,
          opcion => [opcion.nombre],
          'equipo'
        );
        const tipoResuelto = this.resolverElementoCsv(
          tipoCromo,
          this.tiposCromoAlbum,
          opcion => [opcion.nombre],
          'tipo de cromo'
        );
        const edicionResuelta = this.resolverElementoCsv(
          edicion,
          this.ediciones,
          opcion => [opcion.nombre, `${opcion.nombre} (${opcion.temporada})`],
          'edición'
        );

        const filaCsv: FilaCromoCsv = {
          filaCsv: indice + 2,
          numero,
          nombreJugador,
          equipo,
          equipoId: equipoResuelto.id,
          temporada,
          tipoCromo,
          tipoCromoId: tipoResuelto.id,
          edicion,
          edicionId: edicionResuelta.id,
          comentario,
          jugadorId: null,
          jugadorNombreValidado: null,
          jugadoresSugeridos: [],
          esEscudo: normalizarTexto(nombreJugador) === 'escudo',
          error: ''
        };

        if (!numero) filaCsv.error = 'Falta el número del cromo.';
        else if (numero.length > 100) filaCsv.error = 'El número supera los 100 caracteres.';
        if (!nombreJugador) filaCsv.error = [filaCsv.error, 'Falta el nombre del jugador.'].filter(Boolean).join(' ');
        if (!temporada) this.agregarErrorCsv(filaCsv, 'Falta la temporada del jugador en el equipo.');
        if (equipoResuelto.error) this.agregarErrorCsv(filaCsv, equipoResuelto.error);
        if (tipoResuelto.error) this.agregarErrorCsv(filaCsv, tipoResuelto.error);
        if (edicionResuelta.error) this.agregarErrorCsv(filaCsv, edicionResuelta.error);

        return filaCsv;
      });

      this.cargandoCsv = true;
      this.cdr.detectChanges();

      const equipoIds = [...new Set(
        this.csvFilas
          .map(fila => fila.equipoId)
          .filter((id): id is number => id !== null)
      )];
      const plantillas = equipoIds.length
        ? await firstValueFrom(forkJoin(equipoIds.map(id => this.jugadorService.getJugadoresPorEquipo(id))))
        : [];
      const jugadoresPorEquipo = new Map<number, JugadorEquipo[]>();
      equipoIds.forEach((id, indice) => jugadoresPorEquipo.set(id, plantillas[indice] ?? []));

      for (const fila of this.csvFilas) {
        if (fila.jugadorId === null && fila.equipoId !== null && fila.nombreJugador) {
          const candidatos = jugadoresPorEquipo.get(fila.equipoId) ?? [];
          const buscado = normalizarTexto(fila.nombreJugador);
          const candidatosDeTemporada = candidatos.filter(jugador =>
            this.jugadorCoincideTemporada(jugador, fila.temporada)
          );
          const candidatosJugador = candidatosDeTemporada.filter(jugador =>
            buscado === 'escudo'
              ? this.esJugadorEscudo(jugador)
              : true
          );
          const coincidenciasExactas = candidatosJugador.filter(jugador =>
            this.jugadorCoincideNombreExacto(jugador, buscado)
          );
          const exactasPorId = new Map(coincidenciasExactas.map(jugador => [jugador.id, jugador]));

          if (exactasPorId.size === 1) {
            const jugador = [...exactasPorId.values()][0];
            fila.jugadorId = jugador.id;
            fila.jugadorNombreValidado = this.nombreVisibleJugador(jugador);
          } else if (exactasPorId.size > 1) {
            fila.jugadoresSugeridos = [...exactasPorId.values()];
          } else {
            const coincidenciasParciales = candidatosJugador.filter(jugador =>
              this.jugadorCoincideNombreParcial(jugador, buscado)
            );
            fila.jugadoresSugeridos = [...new Map(
              coincidenciasParciales.map(jugador => [jugador.id, jugador])
            ).values()].slice(0, 5);

            if (fila.jugadoresSugeridos.length === 0 && !fila.esEscudo) {
              this.agregarErrorCsv(fila, 'No se encontró el jugador en el equipo y temporada indicados.');
            }
          }
        }
      }

      const cantidadesPorNumero = new Map<string, number>();
      for (const fila of this.csvFilas) {
        if (fila.equipoId === null || fila.tipoCromoId === null || fila.edicionId === null) continue;
        const clave = this.claveCromoCsv(fila);
        cantidadesPorNumero.set(clave, (cantidadesPorNumero.get(clave) ?? 0) + 1);
      }
      for (const fila of this.csvFilas) {
        if (fila.equipoId !== null && fila.tipoCromoId !== null && fila.edicionId !== null
          && (cantidadesPorNumero.get(this.claveCromoCsv(fila)) ?? 0) > 1) {
          this.agregarErrorCsv(fila, `El número «${fila.numero}» está repetido para el mismo equipo, tipo y edición.`);
        }
      }

      if (this.csvFilas.length === 0) {
        this.csvError = 'El CSV no contiene filas con datos.';
      }
    } catch (error: any) {
      this.csvFilas = [];
      this.csvError = error?.message ?? 'No se pudo leer el archivo CSV.';
    } finally {
      this.cargandoCsv = false;
      this.cdr.detectChanges();
    }
  }

  importarCsv(): void {
    if (!this.csvImportable || !this.albumSeleccionado) return;

    this.importandoCsv = true;
    this.csvError = '';
    this.csvMensaje = '';

    const payload = {
      albumId: this.albumSeleccionado.id,
      cromos: this.csvFilas.map(fila => ({
        filaCsv: fila.filaCsv,
        numero: fila.numero,
        equipoId: fila.equipoId!,
        tipoCromoId: fila.tipoCromoId!,
        edicionId: fila.edicionId!,
        jugadorId: fila.jugadorId!,
        esEscudo: fila.esEscudo,
        comentario: fila.comentario
      }))
    };

    this.http.post<{ creados: number; escudosCreados: number }>(`${this.apiUrl}/Cromo/importar-lote`, payload).subscribe({
      next: resultado => {
        this.importandoCsv = false;
        this.csvMensaje = `Se importaron ${resultado.creados} cromos correctamente.`
          + (resultado.escudosCreados
            ? ` Se crearon ${resultado.escudosCreados} jugador(es) especial(es) Escudo.`
            : '');
        this.csvFilas = [];
        this.csvArchivo = '';
        if (this.mostrarTabla) this.verEquipo();
        this.cdr.detectChanges();
      },
      error: respuesta => {
        this.importandoCsv = false;
        const cuerpo = respuesta.error;
        const detalle = Array.isArray(cuerpo?.errores)
          ? cuerpo.errores
              .map((error: any) => `Fila ${error.filaCsv}: ${error.mensaje}`)
              .join('\n')
          : '';
        this.csvError = [cuerpo?.message ?? 'No se pudo importar el CSV.', detalle]
          .filter(Boolean)
          .join('\n');
        this.cdr.detectChanges();
      }
    });
  }

  descargarPlantillaCsv(): void {
    const contenido = '\uFEFFnumero,nombreJugador,equipo,temporada,tipoCromo,edicion,comentario\r\n';
    const url = URL.createObjectURL(new Blob([contenido], { type: 'text/csv;charset=utf-8' }));
    const enlace = document.createElement('a');
    enlace.href = url;
    enlace.download = 'plantilla-cromos-con-temporada.csv';
    enlace.click();
    URL.revokeObjectURL(url);
  }

  private limpiarCsv(): void {
    this.csvArchivo = '';
    this.csvFilas = [];
    this.csvMensaje = '';
    this.csvError = '';
  }

  private buscarColumna(cabeceras: string[], opciones: string[]): number {
    return cabeceras.findIndex(cabecera => opciones.includes(cabecera));
  }

  private resolverElementoCsv<T extends { id: number }>(
    valor: string,
    opciones: T[],
    etiquetas: (opcion: T) => string[],
    descripcion: string
  ): { id: number | null; error: string } {
    const clave = normalizarTexto(valor);
    if (!clave) return { id: null, error: `Falta el ${descripcion}.` };

    const coincidencias = opciones.filter(opcion =>
      etiquetas(opcion).some(etiqueta => normalizarTexto(etiqueta) === clave)
    );
    if (coincidencias.length === 1)
      return { id: coincidencias[0].id, error: '' };
    if (coincidencias.length > 1)
      return { id: null, error: `El valor «${valor}» coincide con más de un ${descripcion}; usa su ID.` };

    if (/^\d+$/.test(valor)) {
      const porId = opciones.find(opcion => opcion.id === Number(valor));
      if (porId) return { id: porId.id, error: '' };
    }

    return { id: null, error: `No se encontró el ${descripcion} «${valor}» en el álbum o catálogo.` };
  }

  private agregarErrorCsv(fila: FilaCromoCsv, mensaje: string): void {
    fila.error = [fila.error, mensaje].filter(Boolean).join(' ');
  }

  private claveCromoCsv(fila: FilaCromoCsv): string {
    return `${fila.equipoId}:${fila.tipoCromoId}:${fila.edicionId}:${normalizarTexto(fila.numero)}`;
  }

  private esJugadorEscudo(jugador: JugadorEquipo): boolean {
    return normalizarTexto(jugador.nombre).includes('escudo')
      || normalizarTexto(jugador.nombreCompleto).includes('escudo');
  }

  private jugadorCoincideTemporada(jugador: JugadorEquipo, temporada: string): boolean {
    return jugador.temporadas.some(etapa => this.etapaCoincideTemporada(etapa, temporada));
  }

  private etapaCoincideTemporada(
    etapa: { temporadaInicio: string; temporadaFin: string },
    temporada: string
  ): boolean {
    const aniosObjetivo = this.aniosDeTemporada(temporada);
    const aniosInicio = this.aniosDeTemporada(etapa.temporadaInicio);

    if (aniosObjetivo.length === 0 || aniosInicio.length === 0) {
      return normalizarTexto(etapa.temporadaInicio) === normalizarTexto(temporada)
        || normalizarTexto(etapa.temporadaFin) === normalizarTexto(temporada);
    }

    const objetivo = aniosObjetivo[0];
    const inicio = aniosInicio[0];
    const aniosFin = this.aniosDeTemporada(etapa.temporadaFin);
    if (objetivo < inicio) return false;
    if (aniosFin.length === 0) return true;

    // Un CSV con un solo año (p. ej. 2026) representa el año natural y puede
    // coincidir con la segunda mitad de una temporada (p. ej. 2025/2026).
    // Si trae una temporada completa, comparamos el año de inicio de temporada.
    const fin = aniosObjetivo.length > 1
      ? aniosFin[0]
      : aniosFin[aniosFin.length - 1];
    return objetivo <= fin;
  }

  private aniosDeTemporada(temporada: string): number[] {
    return (temporada ?? '').match(/\d{4}/g)?.map(anio => Number(anio)) ?? [];
  }

  private jugadorCoincideNombreExacto(jugador: JugadorEquipo, buscado: string): boolean {
    const nombre = normalizarTexto(jugador.nombre);
    const nombreCompleto = normalizarTexto(jugador.nombreCompleto);
    return nombreCompleto === buscado
      || (nombre === buscado && (!nombreCompleto || nombreCompleto === nombre));
  }

  private jugadorCoincideNombreParcial(jugador: JugadorEquipo, buscado: string): boolean {
    const palabrasBuscadas = buscado.split(/\s+/).filter(Boolean);
    if (!palabrasBuscadas.length) return false;

    return [jugador.nombre, jugador.nombreCompleto].some(nombre => {
      const palabrasNombre = normalizarTexto(nombre).split(/\s+/).filter(Boolean);
      return palabrasBuscadas.every(palabra => palabrasNombre.some(palabraNombre =>
        palabraNombre === palabra || (palabra.length >= 4 && palabraNombre.startsWith(palabra))
      ));
    });
  }

  nombreVisibleJugador(jugador: JugadorEquipo): string {
    return jugador.nombreCompleto || jugador.nombre;
  }

  confirmarJugadorCsv(fila: FilaCromoCsv, jugador: JugadorEquipo): void {
    fila.jugadorId = jugador.id;
    fila.jugadorNombreValidado = this.nombreVisibleJugador(jugador);
    fila.jugadoresSugeridos = [];
    this.cdr.detectChanges();
  }

  private parsearCsv(texto: string): string[][] {
    const contenido = texto.replace(/^\uFEFF/, '');
    const primeraLinea = contenido.split(/\r\n|\n|\r/, 1)[0] ?? '';
    const delimitador = (primeraLinea.match(/;/g)?.length ?? 0) > (primeraLinea.match(/,/g)?.length ?? 0)
      ? ';'
      : ',';
    const filas: string[][] = [];
    let fila: string[] = [];
    let campo = '';
    let entreComillas = false;

    for (let indice = 0; indice < contenido.length; indice++) {
      const caracter = contenido[indice];

      if (caracter === '"') {
        if (entreComillas && contenido[indice + 1] === '"') {
          campo += '"';
          indice++;
        } else {
          entreComillas = !entreComillas;
        }
      } else if (caracter === delimitador && !entreComillas) {
        fila.push(campo);
        campo = '';
      } else if ((caracter === '\n' || caracter === '\r') && !entreComillas) {
        fila.push(campo);
        if (fila.some(valor => valor.trim() !== '')) filas.push(fila);
        fila = [];
        campo = '';
        if (caracter === '\r' && contenido[indice + 1] === '\n') indice++;
      } else {
        campo += caracter;
      }
    }

    if (entreComillas) throw new Error('Hay un campo entre comillas que no está cerrado.');

    fila.push(campo);
    if (fila.some(valor => valor.trim() !== '')) filas.push(fila);
    return filas;
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
