import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Pais {
  id: number;
  nombre: string;
}

@Injectable({
  providedIn: 'root'
})
export class PaisService {

  private http = inject(HttpClient);

  private apiUrl = '/api/Paises';

  getPaises(): Observable<Pais[]> {
    return this.http.get<Pais[]>(this.apiUrl);
  }

  crearPais(pais: { nombre: string }): Observable<Pais> {
    return this.http.post<Pais>(this.apiUrl, pais);
  }
}
