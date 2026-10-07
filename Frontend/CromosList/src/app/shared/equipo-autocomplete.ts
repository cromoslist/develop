import { Component, OnInit, Input, forwardRef } from '@angular/core';
import { CommonModule } from '@angular/common';
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
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MATERIAL_IMPORTS } from './material.imports';
import { normalizarTexto } from './texto.util';
import { EquipoService, Equipo } from '../services/equipo.service';

// Autocomplete de equipos con seleccion multiple: cada equipo elegido se
// muestra como chip y el control emite number[] con los ids seleccionados.
@Component({
  selector: 'app-equipo-autocomplete',
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './equipo-autocomplete.html',
  styleUrl: './equipo-autocomplete.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => EquipoAutocompleteComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => EquipoAutocompleteComponent),
      multi: true
    }
  ]
})
export class EquipoAutocompleteComponent implements ControlValueAccessor, Validator, OnInit {

  @Input() label = 'Equipos';
  @Input() placeholder = 'Busca un equipo...';
  @Input() errorText = 'Debes seleccionar al menos un equipo.';

  busqueda = new FormControl('');
  seleccion: Equipo[] = [];

  cargando = true;
  errorCarga = '';

  filtrados$!: Observable<Equipo[]>;

  private equiposSubject = new BehaviorSubject<Equipo[]>([]);
  private textoSubject = new BehaviorSubject<string>('');
  private seleccionSubject = new BehaviorSubject<number[]>([]);

  private ids: number[] = [];
  private control: AbstractControl | null = null;
  private onChange: (ids: number[]) => void = () => {};
  private onTouched: () => void = () => {};

  constructor(private equipoService: EquipoService) {}

  ngOnInit(): void {
    this.filtrados$ = combineLatest([
      this.equiposSubject,
      this.textoSubject,
      this.seleccionSubject
    ]).pipe(
      map(([equipos, texto, ids]) => {
        const t = normalizarTexto(texto);
        const yaElegidos = new Set(ids);

        return equipos.filter(e =>
          !yaElegidos.has(e.id) &&
          (!t || normalizarTexto(e.nombre).includes(t))
        );
      })
    );

    this.busqueda.valueChanges.subscribe(valor => {
      this.textoSubject.next(valor ?? '');
    });

    this.equipoService.getEquipos().subscribe({
      next: equipos => {
        this.equiposSubject.next(equipos);
        this.sincronizarSeleccion();
        this.cargando = false;
      },
      error: () => {
        this.cargando = false;
        this.errorCarga = 'No se han podido cargar los equipos.';
      }
    });
  }

  writeValue(value: number[] | null): void {
    this.ids = (value ?? []).filter(id => typeof id === 'number');
    this.seleccionSubject.next(this.ids);
    this.sincronizarSeleccion();
  }

  registerOnChange(fn: (ids: number[]) => void): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: () => void): void {
    this.onTouched = fn;
  }

  registerOnValidatorChange(): void {
    // Los validadores los define el FormControl del formulario padre.
  }

  // Solo guarda la referencia al control para poder leer su estado y
  // mostrar el error; los errores reales los pone ese mismo control.
  validate(control: AbstractControl): ValidationErrors | null {
    this.control = control;
    return null;
  }

  setDisabledState(isDisabled: boolean): void {
    if (isDisabled) {
      this.busqueda.disable();
    } else {
      this.busqueda.enable();
    }
  }

  elegir(equipo: Equipo): void {
    if (this.seleccion.some(e => e.id === equipo.id)) {
      return;
    }

    this.seleccion = [...this.seleccion, equipo];
    this.busqueda.setValue('');
    this.propagar();
  }

  quitar(equipo: Equipo): void {
    this.seleccion = this.seleccion.filter(e => e.id !== equipo.id);
    this.propagar();
  }

  marcarTocado(): void {
    this.onTouched();
  }

  get muestraError(): boolean {
    if (this.errorCarga) {
      return true;
    }

    return !!this.control?.invalid && !!(this.control.touched || this.control.dirty);
  }

  private sincronizarSeleccion(): void {
    const equipos = this.equiposSubject.value;

    this.seleccion = this.ids
      .map(id => equipos.find(e => e.id === id))
      .filter((e): e is Equipo => !!e);
  }

  private propagar(): void {
    this.ids = this.seleccion.map(e => e.id);
    this.seleccionSubject.next(this.ids);
    this.onChange(this.ids);
    this.onTouched();
  }
}