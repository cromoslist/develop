import { Component, Inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MATERIAL_IMPORTS } from '../../shared/material.imports';
import { EquiposTemporadasEditorComponent } from '../../shared/equipos-temporadas-editor';
import { PaisAutocompleteComponent } from '../../shared/pais-autocomplete';
import { AniosEditorComponent } from '../../shared/anios-editor';
import { Jugador, JugadorService } from '../../services/jugador.service';

export interface EditarJugadorData {
  jugador: Jugador;
}

@Component({
  selector: 'app-jugador-editar',
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatDialogModule,
    MatProgressSpinnerModule,
    MatCheckboxModule,
    EquiposTemporadasEditorComponent,
    PaisAutocompleteComponent,
    AniosEditorComponent,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './jugador-editar.html',
  styleUrl: './jugador-editar.scss'
})
export class JugadorEditarComponent implements OnInit {

  jugadorForm!: FormGroup;

  cargando = false;
  error = '';

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: EditarJugadorData,
    private dialogRef: MatDialogRef<JugadorEditarComponent>,
    private fb: FormBuilder,
    private jugadorService: JugadorService
  ) {}

  ngOnInit(): void {
    const jugador = this.data.jugador;
    const internacional = jugador.internacional ?? false;

    this.jugadorForm = this.fb.group({
      nombre: [
        jugador.nombre,
        [
          Validators.required,
          Validators.maxLength(100)
        ]
      ],
      nombreCompleto: [
        jugador.nombreCompleto ?? '',
        [Validators.maxLength(150)]
      ],
      equipos: [
        jugador.equipos.map(e => ({
          equipoId: e.id,
          temporadaInicio: e.temporadaInicio,
          temporadaFin: e.temporadaFin
        })),
        [Validators.required]
      ],
      paises: [
        (jugador.paises ?? []).map(p => p.id),
        [Validators.required]
      ],
      internacional: [internacional],
      aniosInternacional: [
        [...(jugador.aniosInternacional ?? [])],
        internacional ? [Validators.required] : []
      ]
    });

    this.jugadorForm.get('internacional')?.valueChanges.subscribe(valor => {
      const anios = this.jugadorForm.get('aniosInternacional');

      if (valor) {
        anios?.setValidators([Validators.required]);
      } else {
        anios?.setValue([]);
        anios?.clearValidators();
      }

      anios?.updateValueAndValidity();
    });
  }

  get esInternacional(): boolean {
    return !!this.jugadorForm?.get('internacional')?.value;
  }

  guardar(): void {
    if (this.jugadorForm.invalid) {
      this.jugadorForm.markAllAsTouched();
      return;
    }

    this.cargando = true;
    this.error = '';

    const valor = this.jugadorForm.getRawValue();
    const internacional = !!valor.internacional;

    this.jugadorService
      .editarJugador(this.data.jugador.id, {
        nombre: valor.nombre,
        nombreCompleto: valor.nombreCompleto,
        equipos: valor.equipos ?? [],
        internacional,
        aniosInternacional: internacional ? (valor.aniosInternacional ?? []) : [],
        paises: valor.paises ?? []
      })
      .subscribe({
        next: () => {
          this.cargando = false;
          this.dialogRef.close(true);
        },
        error: () => {
          this.cargando = false;
          this.error = 'No se ha podido actualizar el jugador.';
        }
      });
  }

  cancelar(): void {
    this.dialogRef.close(false);
  }
}
