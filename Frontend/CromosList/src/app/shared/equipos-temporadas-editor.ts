import { Component, OnInit, Input, forwardRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import {
  AbstractControl,
  FormControl,
  ReactiveFormsModule,
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  ValidationErrors,
  Validator
} from '@angular/forms';
import { Observable, BehaviorSubject, combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MATERIAL_IMPORTS } from './material.imports';
import { normalizarTexto } from './texto.util';
import { EquipoService, Equipo } from '../services/equipo.service';
import { JugadorEquipoPayload } from '../services/jugador.service';

interface EquipoSeleccionado {
  id: number;
  nombre: string;
  temporadaInicio: string;
  temporadaFin: string;
}

interface OpcionCrear {
  crear: true;
  nombre: string;
}

// Editor de las etapas de un jugador: se eligen equipos y, para cada uno, se
// indica la temporada de inicio y de fin. Emite JugadorEquipoPayload[].
@Component({
  selector: 'app-equipos-temporadas-editor',
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    MatProgressSpinnerModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './equipos-temporadas-editor.html',
  styleUrl: './equipos-temporadas-editor.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => EquiposTemporadasEditorComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => EquiposTemporadasEditorComponent),
      multi: true
    }
  ]
})
export class EquiposTemporadasEditorComponent implements ControlValueAccessor, Validator, OnInit {

  @Input() label = 'Equipos';
  @Input() placeholder = 'Busca un equipo...';
  @Input() errorText = 'Debes seleccionar al menos un equipo.';

  busqueda = new FormControl('');
  seleccion: EquipoSeleccionado[] = [];

  cargando = true;
  errorCarga = '';
  creando = false;
  errorCreacion = '';

  filtrados$!: Observable<Equipo[]>;

  private equiposSubject = new BehaviorSubject<Equipo[]>([]);
  private textoSubject = new BehaviorSubject<string>('');
  private todosEquipos: Equipo[] = [];
  private pendiente: JugadorEquipoPayload[] = [];
  deshabilitado = false;

  private control: AbstractControl | null = null;
  private onChange: (equipos: JugadorEquipoPayload[]) => void = () => {};
  private onTouched: () => void = () => {};

  constructor(private equipoService: EquipoService) {}

  ngOnInit(): void {
    this.filtrados$ = combineLatest([
      this.equiposSubject,
      this.textoSubject
    ]).pipe(
      map(([equipos, texto]) => {
        const t = normalizarTexto(texto);

        return equipos.filter(e => !t || normalizarTexto(e.nombre).includes(t));
      })
    );

    this.busqueda.valueChanges.subscribe(valor => {
      this.textoSubject.next(valor ?? '');
    });

    this.equipoService.getEquipos().subscribe({
      next: equipos => {
        this.todosEquipos = equipos;
        this.equiposSubject.next(equipos);
        this.aplicarValor();
        this.cargando = false;
      },
      error: () => {
        this.cargando = false;
        this.errorCarga = 'No se han podido cargar los equipos.';
      }
    });
  }

  writeValue(valor: JugadorEquipoPayload[] | null): void {
    this.pendiente = valor ?? [];
    this.aplicarValor();
  }

  registerOnChange(fn: (equipos: JugadorEquipoPayload[]) => void): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: () => void): void {
    this.onTouched = fn;
  }

  registerOnValidatorChange(): void {
    // Los validadores los define el FormControl del formulario padre.
  }

  validate(control: AbstractControl): ValidationErrors | null {
    this.control = control;

    if (this.faltanTemporadas) {
      return { temporadaInicio: true };
    }

    if (this.hayEtapasDuplicadas) {
      return { etapaDuplicada: true };
    }

    return null;
  }

  setDisabledState(isDisabled: boolean): void {
    this.deshabilitado = isDisabled;

    if (isDisabled) {
      this.busqueda.disable();
    } else {
      this.busqueda.enable();
    }
  }

  get faltanTemporadas(): boolean {
    return this.seleccion.some(e => !(e.temporadaInicio ?? '').trim());
  }

  get opcionCrear(): OpcionCrear {
    return { crear: true, nombre: (this.busqueda.value ?? '').toString().trim() };
  }

  get puedeCrear(): boolean {
    const t = normalizarTexto(this.busqueda.value);

    if (!t) {
      return false;
    }

    return !this.todosEquipos.some(e => normalizarTexto(e.nombre) === t);
  }

  onOptionSelected(valor: Equipo | OpcionCrear): void {
    if (this.esOpcionCrear(valor)) {
      this.crear(valor.nombre);
    } else {
      this.elegir(valor);
    }
  }

  crear(nombre: string): void {
    const limpio = nombre.trim();

    if (!limpio || this.creando) {
      return;
    }

    this.creando = true;
    this.errorCreacion = '';

    this.equipoService.crearEquipo({ nombre: limpio }).subscribe({
      next: equipo => {
        this.creando = false;

        if (!this.todosEquipos.some(e => e.id === equipo.id)) {
          this.todosEquipos = [...this.todosEquipos, equipo]
            .sort((a, b) => a.nombre.localeCompare(b.nombre));
          this.equiposSubject.next(this.todosEquipos);
        }

        this.elegir(equipo);
      },
      error: () => {
        this.creando = false;
        this.errorCreacion = 'No se ha podido crear el equipo.';
      }
    });
  }

  private esOpcionCrear(valor: Equipo | OpcionCrear): valor is OpcionCrear {
    return !!valor && (valor as OpcionCrear).crear === true;
  }

  get hayEtapasDuplicadas(): boolean {
    const claves = new Set<string>();

    for (const etapa of this.seleccion) {
      const inicio = (etapa.temporadaInicio ?? '').trim();

      if (!inicio) {
        continue;
      }

      const clave = `${etapa.id}|${normalizarTexto(inicio)}`;

      if (claves.has(clave)) {
        return true;
      }

      claves.add(clave);
    }

    return false;
  }

  elegir(equipo: Equipo): void {
    this.seleccion = [
      ...this.seleccion,
      {
        id: equipo.id,
        nombre: equipo.nombre,
        temporadaInicio: '',
        temporadaFin: ''
      }
    ];

    this.busqueda.setValue('');
    this.propagar();
  }

  quitar(indice: number): void {
    this.seleccion.splice(indice, 1);
    this.seleccion = [...this.seleccion];
    this.propagar();
  }

  propagar(): void {
    this.onChange(this.seleccion.map(e => ({
      equipoId: e.id,
      temporadaInicio: (e.temporadaInicio ?? '').trim(),
      temporadaFin: (e.temporadaFin ?? '').trim()
    })));
    this.onTouched();
  }

  marcarTocado(): void {
    this.onTouched();
  }

  get muestraError(): boolean {
    if (this.errorCarga || this.errorCreacion) {
      return true;
    }

    return !!this.control?.invalid && !!(this.control.touched || this.control.dirty);
  }

  get textoError(): string {
    if (this.errorCarga || this.errorCreacion) {
      return this.errorCarga || this.errorCreacion;
    }

    if (this.faltanTemporadas) {
      return 'Cada equipo necesita una temporada de inicio.';
    }

    if (this.hayEtapasDuplicadas) {
      return 'Hay dos etapas del mismo equipo con la misma temporada de inicio.';
    }

    return this.errorText;
  }

  private aplicarValor(): void {
    this.seleccion = this.pendiente.map(valor => {
      const equipo = this.todosEquipos.find(e => e.id === valor.equipoId);

      return {
        id: valor.equipoId,
        nombre: equipo?.nombre ?? '',
        temporadaInicio: valor.temporadaInicio ?? '',
        temporadaFin: valor.temporadaFin ?? ''
      };
    });
  }
}
