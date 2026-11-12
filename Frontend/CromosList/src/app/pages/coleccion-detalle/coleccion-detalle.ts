import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { AlbumService, AlbumDetalle, AlbumEquipoConCromos, AlbumCromo } from '../../services/album.service';
import { UsuarioAlbumService, ColeccionProgreso } from '../../services/usuario-album.service';
import { UsuarioCromoService } from '../../services/usuario-cromo.service';
import { AuthService } from '../../services/auth.service';
import { CromoEditarComponent } from './cromo-editar';
import { normalizarTexto } from '../../shared/texto.util';

export interface EspecialGrupo {
  tipo: string;
  cromos: AlbumCromo[];
}

export interface ColeccionGrupo {
  id: number | null;
  nombre: string;
  temporadas: string[];
  cromos: AlbumCromo[];
}

export interface FaltaDetectada {
  referencia: string;
  cromo: AlbumCromo;
  confianza: number;
  seleccionada: boolean;
}

@Component({
  selector: 'app-coleccion-detalle',
  imports: [
    CommonModule,
    FormsModule,
    RouterLink,
    MatCardModule,
    MatIconModule,
    MatExpansionModule,
    MatProgressSpinnerModule,
    MatProgressBarModule,
    MatButtonModule,
    MatCheckboxModule,
    MatDialogModule
  ],
  templateUrl: './coleccion-detalle.html',
  styleUrl: './coleccion-detalle.scss'
})
export class ColeccionDetalleComponent implements OnInit {

  album: AlbumDetalle | null = null;
  cargando = true;
  error = '';

  progreso: ColeccionProgreso | null = null;
  marcando = false;
  soloFaltan = false;
  imagenFaltas: File | null = null;
  textoReconocido = '';
  faltasDetectadas: FaltaDetectada[] = [];
  referenciasSinCoincidencia: string[] = [];
  leyendoImagen = false;
  importandoFaltas = false;
  progresoOcr = 0;
  estadoOcr = '';
  errorImportacion = '';
  mensajeImportacion = '';
  hayVistaPrevia = false;

  placeholders = [1, 2, 3, 4];

  especiales: EspecialGrupo[] = [];
  especialesVisibles: EspecialGrupo[] = [];

  colecciones: ColeccionGrupo[] = [];
  coleccionesVisibles: ColeccionGrupo[] = [];

  private miId: number | null = null;
  private tenidosIds = new Set<number>();

  constructor(
    private route: ActivatedRoute,
    private albumService: AlbumService,
    private usuarioAlbumService: UsuarioAlbumService,
    private usuarioCromoService: UsuarioCromoService,
    private authService: AuthService,
    private dialog: MatDialog,
    private cdr: ChangeDetectorRef
  ) {}

  editarCromo(cromo: AlbumCromo): void {
    if (!this.album) return;

    const dialogRef = this.dialog.open(CromoEditarComponent, {
      width: '520px',
      data: { cromo, albumId: this.album.id }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result && this.album) {
        this.cargarDetalle(this.album.id);
      }
    });
  }

  ngOnInit(): void {
    const id = Number(this.route.snapshot.paramMap.get('id'));
    if (!id) {
      this.error = 'ID de álbum no válido.';
      this.cargando = false;
      return;
    }
    this.miId = this.authService.getMiId();
    this.cargarDetalle(id);
  }

  get suscrito(): boolean {
    return this.progreso?.suscrito ?? false;
  }

  get esAdmin(): boolean {
    return this.authService.isAdmin();
  }

  get esPersonalizado(): boolean {
    return this.album?.temporada === 'Personalizado';
  }

  get jugadorNombre(): string {
    if (!this.album) return '';
    return this.album.nombre.replace(/^Álbum de\s+/i, '').trim() || this.album.nombre;
  }

  get jugadorInicial(): string {
    const n = this.jugadorNombre;
    if (!n) return '?';
    const partes = n.split(' ');
    return (partes[0][0] + (partes[1]?.[0] ?? '')).toUpperCase();
  }

  get totalCromos(): number {
    if (!this.album) return 0;
    return this.album.equipos.reduce((suma, e) => suma + e.cromos.length, 0);
  }

  trackEquipo(_: number, equipo: any): number {
    return equipo.id;
  }

  trackTipo(_: number, grupo: EspecialGrupo): string {
    return grupo.tipo;
  }

  // Tipos que se muestran en la rejilla del equipo junto a los basicos.
  // Los entrenadores no van aparte: comparten grupo con basicos y bajas.
  private readonly tiposEnBase = [
    'basica',
    'coloca',
    'baja',
    'entrenador',
    'entrenadores',
    'cuerpo tecnico',
    'ficha bis',
    'fichas bis',
    'estadio'
  ];

  esBasico(tipo: string): boolean {
    const t = normalizarTexto(tipo);
    return this.tiposEnBase.some(base => t === base || t.startsWith(base));
  }

  cromosDeEquipo(equipo: AlbumEquipoConCromos): AlbumCromo[] {
    return equipo.cromos
      .filter(c => this.esBasico(c.tipoCromo.nombre))
      .sort((a, b) => this.compararNumero(a, b));
  }

  cromosVisibles(equipo: AlbumEquipoConCromos): AlbumCromo[] {
    const cromos = this.cromosDeEquipo(equipo);
    return this.soloFaltan
      ? cromos.filter(c => !this.loTiene(c))
      : cromos;
  }

  trackColeccion(_: number, grupo: ColeccionGrupo): number | null {
    return grupo.id;
  }

  cambiarFiltro(checked: boolean): void {
    this.soloFaltan = checked;
    this.aplicarFiltro();
    this.cdr.detectChanges();
  }

  mostrarEdicion(cromo: AlbumCromo): boolean {
    return cromo.edicion != null && cromo.edicion.id !== 1;
  }

  puedeMarcar(): boolean {
    return this.suscrito && !this.marcando;
  }

  loTiene(cromo: AlbumCromo): boolean {
    return this.tenidosIds.has(cromo.id);
  }

  progresoPct(): number {
    if (!this.progreso || !this.progreso.totalCromos) return 0;
    return Math.round((this.progreso.tenidos / this.progreso.totalCromos) * 100);
  }

  progresoEquipo(equipo: AlbumEquipoConCromos): {
    tenidos: number;
    total: number;
    faltan: number;
    porcentaje: number;
  } {
    const total = equipo.cromos.length;
    const tenidos = equipo.cromos.reduce((cantidad, cromo) => cantidad + (this.loTiene(cromo) ? 1 : 0), 0);

    return {
      tenidos,
      total,
      faltan: total - tenidos,
      porcentaje: total ? Math.round((tenidos / total) * 100) : 0
    };
  }

  seleccionarImagenFaltas(evento: Event): void {
    const input = evento.target as HTMLInputElement;
    const archivo = input.files?.[0] ?? null;
    input.value = '';

    this.errorImportacion = '';
    this.mensajeImportacion = '';
    this.imagenFaltas = null;
    this.faltasDetectadas = [];
    this.referenciasSinCoincidencia = [];
    this.hayVistaPrevia = false;
    this.textoReconocido = '';

    if (!archivo) return;
    if (!archivo.type.startsWith('image/')) {
      this.errorImportacion = 'Selecciona un archivo de imagen.';
      return;
    }
    if (archivo.size > 12 * 1024 * 1024) {
      this.errorImportacion = 'La imagen debe pesar menos de 12 MB.';
      return;
    }

    this.imagenFaltas = archivo;
  }

  async leerImagenFaltas(): Promise<void> {
    if (!this.imagenFaltas || this.leyendoImagen) return;

    this.leyendoImagen = true;
    this.progresoOcr = 0;
    this.estadoOcr = 'Preparando el reconocimiento…';
    this.errorImportacion = '';
    this.mensajeImportacion = '';

    let worker: import('tesseract.js').Worker | null = null;
    try {
      type TesseractModule = {
        createWorker?: typeof import('tesseract.js').createWorker;
        default?: { createWorker: typeof import('tesseract.js').createWorker };
      };
      const tesseract = await import('tesseract.js') as unknown as TesseractModule;
      const createWorker = tesseract.createWorker ?? tesseract.default?.createWorker;
      if (!createWorker) {
        throw new Error('No se pudo inicializar el motor OCR.');
      }
      worker = await createWorker('spa+eng', undefined, {
        logger: mensaje => {
          if (mensaje.status === 'recognizing text') {
            this.progresoOcr = Math.round(mensaje.progress * 100);
            this.estadoOcr = 'Leyendo el texto de la imagen…';
            this.cdr.detectChanges();
          }
        }
      });
      const resultado = await worker.recognize(this.imagenFaltas);
      this.textoReconocido = resultado.data.text.trim();
      if (!this.textoReconocido) {
        this.errorImportacion = 'No se encontró texto legible. Prueba con una imagen más nítida.';
      } else {
        this.estadoOcr = 'Texto reconocido. Revísalo y analiza las coincidencias.';
      }
    } catch (error) {
      console.error('Error al reconocer la imagen de faltas', error);
      this.errorImportacion = 'No se pudo leer la imagen. Comprueba tu conexión e inténtalo de nuevo.';
      this.estadoOcr = '';
    } finally {
      await worker?.terminate();
      this.leyendoImagen = false;
      this.cdr.detectChanges();
    }
  }

  analizarTextoFaltas(): void {
    this.errorImportacion = '';
    this.mensajeImportacion = '';
    const referencias = this.separarReferencias(this.textoReconocido);
    const imagenSinFaltas = /\bfaltas?\s*:?\s*0\b/i.test(this.textoReconocido);
    if (!referencias.length && !imagenSinFaltas) {
      this.errorImportacion = 'No hay referencias para analizar. Comprueba el texto reconocido.';
      return;
    }

    const cromos = this.album?.equipos.flatMap(equipo => equipo.cromos) ?? [];
    const cromosUnicos = [...new Map(cromos.map(cromo => [cromo.id, cromo])).values()];
    const detectadas: FaltaDetectada[] = [];
    const sinCoincidencia: string[] = [];
    const idsAsignados = new Set<number>();

    for (const referencia of referencias) {
      const resultados = cromosUnicos
        .map(cromo => ({ cromo, confianza: this.puntuacionCoincidencia(referencia, cromo) }))
        .sort((a, b) => b.confianza - a.confianza);
      const mejor = resultados[0];
      const segundo = resultados[1];
      const noAmbigua = !segundo || mejor.confianza - segundo.confianza >= 8;

      if (mejor && mejor.confianza >= 60 && noAmbigua && !idsAsignados.has(mejor.cromo.id)) {
        detectadas.push({
          referencia,
          cromo: mejor.cromo,
          confianza: mejor.confianza,
          seleccionada: true
        });
        idsAsignados.add(mejor.cromo.id);
      } else {
        sinCoincidencia.push(referencia);
      }
    }

    this.faltasDetectadas = detectadas;
    this.referenciasSinCoincidencia = sinCoincidencia;
    this.hayVistaPrevia = true;
  }

  cambiarSeleccionFalta(falta: FaltaDetectada, seleccionada: boolean): void {
    falta.seleccionada = seleccionada;
  }

  aplicarImportacionFaltas(): void {
    if (!this.album || !this.miId || !this.hayVistaPrevia || this.importandoFaltas) return;

    const idsFaltantes = this.faltasDetectadas
      .filter(falta => falta.seleccionada)
      .map(falta => falta.cromo.id);
    const detalleNoReconocidos = this.referenciasSinCoincidencia.length > 0
      ? `Hay ${this.referenciasSinCoincidencia.length} referencia(s) sin asociar. `
      : '';
    const continuar = confirm(
      `${detalleNoReconocidos}Se importarán ${idsFaltantes.length} faltas y los demás cromos se marcarán como conseguidos. ¿Continuar?`
    );
    if (!continuar) return;

    this.importandoFaltas = true;
    this.errorImportacion = '';
    this.mensajeImportacion = '';
    this.usuarioCromoService.importarFaltas(this.album.id, idsFaltantes).subscribe({
      next: () => {
        this.importandoFaltas = false;
        this.mensajeImportacion = `Importación completada: ${idsFaltantes.length} faltas aplicadas.`;
        this.cargarProgreso();
        this.cdr.detectChanges();
      },
      error: () => {
        this.importandoFaltas = false;
        this.errorImportacion = 'No se pudieron importar las faltas. No se han aplicado los cambios.';
        this.cdr.detectChanges();
      }
    });
  }

  limpiarImportacionFaltas(): void {
    this.imagenFaltas = null;
    this.textoReconocido = '';
    this.faltasDetectadas = [];
    this.referenciasSinCoincidencia = [];
    this.hayVistaPrevia = false;
    this.errorImportacion = '';
    this.mensajeImportacion = '';
    this.estadoOcr = '';
  }

  private separarReferencias(texto: string): string[] {
    return texto
      .replace(/\r/g, '\n')
      .replace(/\n+/g, ',')
      .split(/[;,]+/)
      .map(referencia => referencia
        .replace(/^\s*faltas?\s*\d*\s*/i, '')
        .replace(/\beditar\s+faltas?\b/gi, '')
        .replace(/[|*]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim())
      .filter(referencia => referencia.length >= 3);
  }

  private puntuacionCoincidencia(referencia: string, cromo: AlbumCromo): number {
    const texto = this.normalizarReferencia(referencia);
    const jugador = this.normalizarReferencia(cromo.jugador?.nombre);
    const numero = this.normalizarReferencia(cromo.numero);
    const tipo = this.normalizarReferencia(cromo.tipoCromo?.nombre);
    const equipo = this.normalizarReferencia(cromo.equipo?.nombre);
    const edicion = this.normalizarReferencia(cromo.edicion?.nombre);
    let puntuacion = 0;

    if (jugador && texto.includes(jugador)) {
      puntuacion += 72;
    } else if (jugador) {
      const palabras = jugador.split(' ').filter(palabra => palabra.length > 2);
      const encontradas = palabras.filter(palabra => texto.split(' ').includes(palabra)).length;
      if (palabras.length) puntuacion += (encontradas / palabras.length) * 55;
    }

    if (numero && texto.includes(numero)) puntuacion += 30;
    if (tipo && texto.includes(tipo)) puntuacion += 18;
    if (equipo && texto.includes(equipo)) puntuacion += 10;
    if (edicion && edicion.length > 2 && texto.includes(edicion)) puntuacion += 12;

    return Math.min(100, Math.round(puntuacion));
  }

  private normalizarReferencia(texto: string | null | undefined): string {
    return normalizarTexto(texto)
      .replace(/[^a-z0-9]+/g, ' ')
      .trim()
      .replace(/\s+/g, ' ');
  }

  alternarCromo(cromo: AlbumCromo): void {
    if (!this.miId || !this.suscrito || this.marcando) return;

    const tiene = this.loTiene(cromo);
    this.marcando = true;

    const operacion = tiene
      ? this.usuarioCromoService.desmarcarCromo(this.miId, cromo.id)
      : this.usuarioCromoService.marcarCromo(this.miId, cromo.id);

    operacion.subscribe({
      next: () => {
        this.marcando = false;
        this.cargarProgreso();
      },
      error: () => {
        this.marcando = false;
        this.error = 'No se ha podido actualizar el cromo.';
        this.cdr.detectChanges();
      }
    });
  }

  apuntarse(): void {
    if (!this.miId || !this.album || this.marcando) return;
    this.marcando = true;

    this.usuarioAlbumService.suscribir({ usuarioId: this.miId, albumId: this.album.id }).subscribe({
      next: () => {
        this.marcando = false;
        this.cargarProgreso();
      },
      error: () => {
        this.marcando = false;
        this.error = 'No se ha podido apuntar a la colección.';
        this.cdr.detectChanges();
      }
    });
  }

  private compararNumero(a: AlbumCromo, b: AlbumCromo): number {
    return this.numeroValor(a.numero) - this.numeroValor(b.numero);
  }

  private numeroValor(numero: string): number {
    const m = (numero || '').match(/\d+/);
    return m ? parseInt(m[0], 10) : Number.MAX_SAFE_INTEGER;
  }

  private construirEspeciales(data: AlbumDetalle): void {
    const grupos = new Map<number, AlbumCromo[]>();
    for (const equipo of data.equipos) {
      for (const cromo of equipo.cromos) {
        if (this.esBasico(cromo.tipoCromo.nombre)) continue;
        if (!grupos.has(cromo.tipoCromo.id)) grupos.set(cromo.tipoCromo.id, []);
        grupos.get(cromo.tipoCromo.id)!.push(cromo);
      }
    }

    const ordenados = data.tiposCromo
      .slice()
      .sort((a, b) => a.orden - b.orden);

    this.especiales = [];

    for (const tipo of ordenados) {
      const cromos = grupos.get(tipo.id);
      if (!cromos || cromos.length === 0) continue;
      this.especiales.push({
        tipo: tipo.nombre,
        cromos: cromos.sort((a, b) => this.compararNumero(a, b))
      });
    }

    for (const [id, cromos] of grupos) {
      if (ordenados.some(t => t.id === id)) continue;
      this.especiales.push({
        tipo: cromos[0].tipoCromo.nombre,
        cromos: cromos.sort((a, b) => this.compararNumero(a, b))
      });
    }
  }

  private construirColecciones(data: AlbumDetalle): void {
    const grupos = new Map<number | null, ColeccionGrupo>();

    for (const equipo of data.equipos) {
      for (const cromo of equipo.cromos) {
        const id = cromo.coleccion?.id ?? null;
        const nombre = cromo.coleccion?.nombre ?? 'Sin colección';

        if (!grupos.has(id)) {
          grupos.set(id, { id, nombre, temporadas: [], cromos: [] });
        }
        grupos.get(id)!.cromos.push(cromo);
      }
    }

    const ordenados: ColeccionGrupo[] = [];

    for (const grupo of grupos.values()) {
      grupo.cromos.sort((a, b) => this.compararNumero(a, b));
      grupo.temporadas = this.temporadasDe(grupo.cromos);
      ordenados.push(grupo);
    }

    this.colecciones = ordenados.sort((a, b) => a.nombre.localeCompare(b.nombre));
  }

  private temporadasDe(cromos: AlbumCromo[]): string[] {
    const temporadas = new Set<string>();

    for (const cromo of cromos) {
      const temporada = cromo.temporada?.trim() || cromo.publicacion?.temporada?.trim();
      if (temporada) temporadas.add(temporada);
    }

    return [...temporadas].sort((a, b) => a.localeCompare(b));
  }

  temporadaGrupo(grupo: ColeccionGrupo): string {
    if (grupo.temporadas.length === 0) return '';
    if (grupo.temporadas.length === 1) return grupo.temporadas[0];
    return `${grupo.temporadas[0]} +${grupo.temporadas.length - 1}`;
  }

  private aplicarFiltro(): void {
    this.especialesVisibles = this.soloFaltan
      ? this.especiales
          .map(grupo => ({
            tipo: grupo.tipo,
            cromos: grupo.cromos.filter(c => !this.loTiene(c))
          }))
          .filter(grupo => grupo.cromos.length > 0)
      : this.especiales;

    this.coleccionesVisibles = this.soloFaltan
      ? this.colecciones
          .map(grupo => ({
            id: grupo.id,
            nombre: grupo.nombre,
            temporadas: grupo.temporadas,
            cromos: grupo.cromos.filter(c => !this.loTiene(c))
          }))
          .filter(grupo => grupo.cromos.length > 0)
      : this.colecciones;
  }

  private cargarDetalle(id: number): void {
    this.cargando = true;
    this.albumService.getAlbumConCromos(id).subscribe({
      next: data => {
        this.album = data;
        this.construirEspeciales(data);
        this.construirColecciones(data);
        this.aplicarFiltro();
        this.cargando = false;
        this.cdr.detectChanges();

        if (this.miId) {
          this.cargarProgreso();
        }
      },
      error: () => {
        this.error = 'No se ha podido cargar el álbum.';
        this.cargando = false;
        this.cdr.detectChanges();
      }
    });
  }

  private cargarProgreso(): void {
    if (!this.miId || !this.album) return;

    this.usuarioAlbumService.getProgresoAlbum(this.miId, this.album.id).subscribe({
      next: data => {
        this.progreso = data;
        this.tenidosIds = new Set(data.cromosTenidos);
        this.aplicarFiltro();
        this.cdr.detectChanges();
      },
      error: () => {
        this.progreso = null;
        this.cdr.detectChanges();
      }
    });
  }
}
