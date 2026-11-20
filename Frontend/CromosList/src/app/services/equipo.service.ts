import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Equipo {
  id: number;
  nombre: string;
  transfermarktId?: string | null;
}

@Injectable({
  providedIn: 'root'
})
export class EquipoService {

  private http = inject(HttpClient);

  private apiUrl = '/api/Equipos';

  getEquipos(): Observable<Equipo[]> {
    return this.http.get<Equipo[]>(this.apiUrl);
  }

  crearEquipo(equipo: { nombre: string; transfermarktId?: string | null }): Observable<Equipo> {
    return this.http.post<Equipo>(this.apiUrl, equipo);
  }

  editarEquipo(id: number, equipo: { nombre: string; transfermarktId?: string | null }): Observable<Equipo> {
    return this.http.put<Equipo>(`${this.apiUrl}/${id}`, equipo);
  }

  crearAlbumPersonalizado(id: number): Observable<{ albumId: number; nombre: string }> {
    return this.http.post<{ albumId: number; nombre: string }>(
      `${this.apiUrl}/${id}/album-personalizado`,
      {}
    );
  }
}
