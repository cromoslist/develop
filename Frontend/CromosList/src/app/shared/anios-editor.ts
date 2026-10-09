import { Component, Input, forwardRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import {
  FormControl,
  ReactiveFormsModule,
  ControlValueAccessor,
  NG_VALUE_ACCESSOR
} from '@angular/forms';
import { MatChipsModule } from '@angular/material/chips';
import { MATERIAL_IMPORTS } from './material.imports';

// Editor de años (lista de enteros): cada año se muestra como chip y el
// control emite number[].
@Component({
  selector: 'app-anios-editor',
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatChipsModule,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './anios-editor.html',
  styleUrl: './anios-editor.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => AniosEditorComponent),
      multi: true
    }
  ]
})
export class AniosEditorComponent implements ControlValueAccessor {

  @Input() label = 'Años';
  @Input() placeholder = 'Ej. 2010';

  entrada = new FormControl('', { nonNullable: true });
  anios: number[] = [];
  deshabilitado = false;

  private onChange: (valor: number[]) => void = () => {};
  private onTouched: () => void = () => {};

  writeValue(valor: number[] | null): void {
    this.anios = [...(valor ?? [])].sort((a, b) => a - b);
  }

  registerOnChange(fn: (valor: number[]) => void): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: () => void): void {
    this.onTouched = fn;
  }

  setDisabledState(estaDeshabilitado: boolean): void {
    this.deshabilitado = estaDeshabilitado;

    if (estaDeshabilitado) {
      this.entrada.disable();
    } else {
      this.entrada.enable();
    }
  }

  anadir(texto?: string): void {
    const valor = Number((texto ?? this.entrada.value ?? '').toString().trim());

    if (Number.isInteger(valor) && valor >= 1900 && valor <= 2100) {
      if (!this.anios.includes(valor)) {
        this.anios = [...this.anios, valor].sort((a, b) => a - b);
        this.onChange(this.anios);
      }
    }

    this.entrada.setValue('');
    this.onTouched();
  }

  quitar(anio: number): void {
    this.anios = this.anios.filter(a => a !== anio);
    this.onChange(this.anios);
    this.onTouched();
  }
}
