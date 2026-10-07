import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface CromoTenido {
  id: number;
  numero: string;
  jugador: { id: number; nombre: string };
  equipo: { id: number; nombre: string };
  edicion: { id: number; nombre: string };
  tipoCromo: { id: number; nombre: string };
}

@Injectable({
  providedIn: 'root'
})
export class UsuarioCromoService {

  private http = inject(HttpClient);

  private apiUrl = '/api/UsuariosCromos';

  listarCromos(usuarioId: number): Observable<CromoTenido[]> {
    return this.http.get<CromoTenido[]>(`${this.apiUrl}/${usuarioId}`);
  }

  marcarCromo(usuarioId: number, cromoId: number): Observable<unknown> {
    return this.http.post(this.apiUrl, { usuarioId, cromoId });
  }

  desmarcarCromo(usuarioId: number, cromoId: number): Observable<void> {
    return this.http.delete<void>(`${this.apiUrl}/${usuarioId}/${cromoId}`);
  }
}