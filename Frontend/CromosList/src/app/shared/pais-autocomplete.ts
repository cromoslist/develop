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
import { PaisService, Pais } from '../services/pais.service';

interface OpcionCrear {
  crear: true;
  nombre: string;
}

// Autocomplete de países con selección múltiple: cada país elegido se muestra
// como chip, el control emite number[] con los ids y se pueden crear países
// nuevos escribiéndolos en el buscador.
@Component({
  selector: 'app-pais-autocomplete',
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './pais-autocomplete.html',
  styleUrl: './pais-autocomplete.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => PaisAutocompleteComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => PaisAutocompleteComponent),
      multi: true
    }
  ]
})
export class PaisAutocompleteComponent implements ControlValueAccessor, Validator, OnInit {

  @Input() label = 'Países';
  @Input() placeholder = 'Busca o escribe un país...';
  @Input() errorText = 'Debes seleccionar al menos un país.';

  busqueda = new FormControl('');
  seleccion: Pais[] = [];

  cargando = true;
  creando = false;
  errorCarga = '';
  errorCreacion = '';

  filtrados$!: Observable<Pais[]>;

  private paisesSubject = new BehaviorSubject<Pais[]>([]);
  private textoSubject = new BehaviorSubject<string>('');
  private seleccionSubject = new BehaviorSubject<number[]>([]);

  private ids: number[] = [];
  private control: AbstractControl | null = null;
  private onChange: (ids: number[]) => void = () => {};
  private onTouched: () => void = () => {};

  constructor(private paisService: PaisService) {}

  ngOnInit(): void {
    this.filtrados$ = combineLatest([
      this.paisesSubject,
      this.textoSubject,
      this.seleccionSubject
    ]).pipe(
      map(([paises, texto, ids]) => {
        const t = normalizarTexto(texto);
        const yaElegidos = new Set(ids);

        return paises.filter(p =>
          !yaElegidos.has(p.id) &&
          (!t || normalizarTexto(p.nombre).includes(t))
        );
      })
    );

    this.busqueda.valueChanges.subscribe(valor => {
      this.textoSubject.next(typeof valor === 'string' ? valor : '');
    });

    this.paisService.getPaises().subscribe({
      next: paises => {
        this.paisesSubject.next(paises);
        this.sincronizarSeleccion();
        this.cargando = false;
      },
      error: () => {
        this.cargando = false;
        this.errorCarga = 'No se han podido cargar los países.';
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

  get opcionCrear(): OpcionCrear {
    return { crear: true, nombre: (this.busqueda.value ?? '').toString().trim() };
  }

  get puedeCrear(): boolean {
    const t = normalizarTexto(this.busqueda.value);
    if (!t) {
      return false;
    }

    return !this.paisesSubject.value.some(p => normalizarTexto(p.nombre) === t);
  }

  onOptionSelected(valor: Pais | OpcionCrear): void {
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

    this.paisService.crearPais({ nombre: limpio }).subscribe({
      next: pais => {
        this.creando = false;

        if (!this.paisesSubject.value.some(p => p.id === pais.id)) {
          const lista = [...this.paisesSubject.value, pais]
            .sort((a, b) => a.nombre.localeCompare(b.nombre));
          this.paisesSubject.next(lista);
        }

        this.elegir(pais);
      },
      error: () => {
        this.creando = false;
        this.errorCreacion = 'No se ha podido crear el país.';
      }
    });
  }

  elegir(pais: Pais): void {
    if (this.seleccion.some(p => p.id === pais.id)) {
      return;
    }

    this.seleccion = [...this.seleccion, pais];
    this.busqueda.setValue('');
    this.propagar();
  }

  quitar(pais: Pais): void {
    this.seleccion = this.seleccion.filter(p => p.id !== pais.id);
    this.propagar();
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

  private esOpcionCrear(valor: Pais | OpcionCrear): valor is OpcionCrear {
    return !!valor && (valor as OpcionCrear).crear === true;
  }

  private sincronizarSeleccion(): void {
    const paises = this.paisesSubject.value;

    this.seleccion = this.ids
      .map(id => paises.find(p => p.id === id))
      .filter((p): p is Pais => !!p);
  }

  private propagar(): void {
    this.ids = this.seleccion.map(p => p.id);
    this.seleccionSubject.next(this.ids);
    this.onChange(this.ids);
    this.onTouched();
  }
}
