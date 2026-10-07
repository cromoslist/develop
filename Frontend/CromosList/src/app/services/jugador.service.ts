import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Jugador {
  id: number;
  nombre: string;
  nombreCompleto: string;
  equipos: { id: number; nombre: string }[];
}

// Respuesta de /api/Equipos/{id}/Jugadores: projection reduced.
export interface JugadorEquipo {
  id: number;
  nombre: string;
  nombreCompleto: string;
}

export interface JugadorPayload {
  nombre: string;
  nombreCompleto?: string;
  equipos: number[];
}

export interface JugadorCromo {
  id: number;
  numero: string;
  equipo: { id: number; nombre: string };
  edicion: { id: number; nombre: string };
  tipoCromo: { id: number; nombre: string };
  tenido: boolean;
}

export interface JugadorColeccion {
  id: number;
  nombre: string;
  temporada: string;
  editorial: { id: number; nombre: string };
  cromos: JugadorCromo[];
}

export interface JugadorCromosDetalle {
  id: number;
  nombre: string;
  totalCromos: number;
  colecciones: JugadorColeccion[];
}

@Injectable({
  providedIn: 'root'
})
export class JugadorService {

  private http = inject(HttpClient);

  private apiUrl = '/api/Jugadores';

  getJugadores(): Observable<Jugador[]> {
    return this.http.get<Jugador[]>(this.apiUrl);
  }

  getJugadoresPorEquipo(equipoId: number): Observable<JugadorEquipo[]> {
    return this.http.get<JugadorEquipo[]>(`/api/Equipos/${equipoId}/Jugadores`);
  }

  // Trae el jugador con la lista completa de sus equipos.
  getJugadoresDeEquipo(equipoId: number): Observable<Jugador[]> {
    return this.http.get<Jugador[]>(this.apiUrl, { params: { equipoId } });
  }

  getCromos(id: number): Observable<JugadorCromosDetalle> {
    return this.http.get<JugadorCromosDetalle>(`${this.apiUrl}/${id}/Cromos`);
  }

  crearAlbumPersonalizado(id: number): Observable<{ albumId: number; nombre: string }> {
    return this.http.post<{ albumId: number; nombre: string }>(
      `${this.apiUrl}/${id}/album-personalizado`,
      {}
    );
  }

  crearJugador(jugador: JugadorPayload): Observable<Jugador> {
    return this.http.post<Jugador>(this.apiUrl, jugador);
  }

  editarJugador(id: number, jugador: JugadorPayload): Observable<Jugador> {
    return this.http.put<Jugador>(`${this.apiUrl}/${id}`, jugador);
  }

  eliminarJugador(id: number): Observable<void> {
    return this.http.delete<void>(`${this.apiUrl}/${id}`);
  }
}
