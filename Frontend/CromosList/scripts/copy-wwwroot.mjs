import { cp, mkdir, readdir, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const raiz = join(dirname(fileURLToPath(import.meta.url)), '..');
const origen = join(raiz, 'dist', 'CromosList', 'browser');
const destino = join(raiz, '..', '..', 'Backend', 'CromosList', 'wwwroot');

const ficheros = await readdir(origen);

await mkdir(destino, { recursive: true });

// Los bundles llevan hash en el nombre, asi que hay que borrar los antiguos
// de wwwroot para que no se vaya llenando de main-*.js obsoletos.
const existentes = await readdir(destino);
const nuevos = new Set(ficheros);

for (const f of existentes) {
  if (/^(main|styles)-[^.]+\.(js|css)$/.test(f) && !nuevos.has(f)) {
    await rm(join(destino, f), { force: true });
  }
}

await cp(origen, destino, { recursive: true, force: true });

console.log(`Frontend copiado a ${destino} (${ficheros.length} ficheros).`);