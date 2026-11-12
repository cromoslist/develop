import { Component, inject } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { TemaService } from '../services/tema.service';

@Component({
  selector: 'app-theme-toggle',
  imports: [MatButtonModule, MatIconModule],
  template: `
    <button
      mat-icon-button
      class="theme-toggle-button"
      [attr.aria-label]="tema.tema() === 'dark' ? 'Cambiar a tema claro' : 'Cambiar a tema oscuro'"
      [title]="tema.tema() === 'dark' ? 'Cambiar a tema claro' : 'Cambiar a tema oscuro'"
      (click)="tema.alternar()">
      <mat-icon>{{ tema.tema() === 'dark' ? 'light_mode' : 'dark_mode' }}</mat-icon>
    </button>
  `,
  styles: `
    :host {
      display: inline-flex;
      flex: 0 0 auto;
    }

    .theme-toggle-button {
      border-radius: 12px;
    }
  `
})
export class ThemeToggleComponent {
  tema = inject(TemaService);
}
