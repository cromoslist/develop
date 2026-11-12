import { DOCUMENT } from '@angular/common';
import { inject, Injectable, signal } from '@angular/core';

type Tema = 'light' | 'dark';

@Injectable({ providedIn: 'root' })
export class TemaService {
  private document = inject(DOCUMENT);
  tema = signal<Tema>(this.obtenerTemaInicial());

  constructor() {
    this.aplicarTema(this.tema());
  }

  alternar(): void {
    const siguiente: Tema = this.tema() === 'dark' ? 'light' : 'dark';
    this.aplicarTema(siguiente);

    try {
      this.document.defaultView?.localStorage.setItem('cromoslist-theme', siguiente);
    } catch {
      // El tema sigue activo durante esta sesión si el navegador bloquea el almacenamiento.
    }
  }

  private obtenerTemaInicial(): Tema {
    try {
      const guardado = this.document.defaultView?.localStorage.getItem('cromoslist-theme');
      if (guardado === 'light' || guardado === 'dark') {
        return guardado;
      }
    } catch {
      // Si no se puede leer el almacenamiento, se consulta la preferencia del dispositivo.
    }

    return this.document.defaultView?.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light';
  }

  private aplicarTema(tema: Tema): void {
    this.tema.set(tema);
    this.document.documentElement.setAttribute('data-theme', tema);
  }
}
