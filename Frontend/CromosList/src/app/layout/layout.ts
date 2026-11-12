import { Component, inject, signal } from '@angular/core';
import { Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { BreakpointObserver } from '@angular/cdk/layout';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MATERIAL_IMPORTS } from '../shared/material.imports';
import { ThemeToggleComponent } from '../shared/theme-toggle';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-layout',
  imports: [
    RouterOutlet,
    RouterLink,
    RouterLinkActive,
    ThemeToggleComponent,
    ...MATERIAL_IMPORTS
  ],
  templateUrl: './layout.html',
  styleUrl: './layout.scss'
})
export class Layout {
  auth = inject(AuthService);
  private router = inject(Router);
  isCompact = signal(false);

  constructor() {
    inject(BreakpointObserver)
      .observe('(max-width: 960px)')
      .pipe(takeUntilDestroyed())
      .subscribe(({ matches }) => this.isCompact.set(matches));
  }

  closeOnCompact(sidenav: { close: () => void }): void {
    if (this.isCompact()) {
      sidenav.close();
    }
  }

  logout(): void {
    this.auth.logout();
    this.router.navigate(['/login']);
  }
}
