// Normaliza texto para comparar nombres sin tildes, mayusculas ni espacios
// sobrantes: 'Escudo R. Madrid' -> 'escudo r. madrid'.
export function normalizarTexto(texto: string | null | undefined): string {
  return (texto ?? '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}