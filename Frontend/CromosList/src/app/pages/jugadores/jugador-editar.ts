import { Component, Inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MATERIAL_IMPORTS } from '../../shared/material.imports';
import { EquipoAutocompleteComponent } from '../../shared/equipo-autocomplete';
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
    EquipoAutocompleteComponent,
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
        jugador.equipos.map(e => e.id),
        [Validators.required]
      ]
    });
  }

  guardar(): void {
    if (this.jugadorForm.invalid) {
      this.jugadorForm.markAllAsTouched();
      return;
    }

    this.cargando = true;
    this.error = '';

    this.jugadorService
      .editarJugador(this.data.jugador.id, {
        nombre: this.jugadorForm.get('nombre')?.value,
        nombreCompleto: this.jugadorForm.get('nombreCompleto')?.value,
        equipos: this.jugadorForm.get('equipos')?.value
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