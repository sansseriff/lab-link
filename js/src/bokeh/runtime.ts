export type BokehNamespace = any

declare global {
  interface Window {
    Bokeh?: BokehNamespace
  }
}

export const BOKEH_VERSION = '3.8.2'
export const BOKEH_FILES = [
  `bokeh-${BOKEH_VERSION}.min.js`,
  `bokeh-gl-${BOKEH_VERSION}.min.js`,
  `bokeh-widgets-${BOKEH_VERSION}.min.js`,
  `bokeh-tables-${BOKEH_VERSION}.min.js`,
  `bokeh-mathjax-${BOKEH_VERSION}.min.js`,
  `bokeh-api-${BOKEH_VERSION}.min.js`,
]

const _scriptPromises = new Map<string, Promise<void>>()
let _bokehPromise: Promise<BokehNamespace> | null = null

/** Returns asset base URLs to try in order. */
export function getBokehAssetBases(): string[] {
  const { protocol, hostname, port } = window.location
  // Always try the current origin first — in dev, Vite proxies /assets → backend.
  const bases = [`${protocol}//${hostname}${port ? `:${port}` : ''}/assets`]
  // Explicit fallback for Vite dev ports in case the proxy isn't configured.
  if (port === '5173' || port === '4173' || port === '5174') {
    bases.push('http://localhost:8000/assets')
  }
  return bases
}

function loadScriptOnce(src: string): Promise<void> {
  const existing = _scriptPromises.get(src)
  if (existing) return existing

  const promise = new Promise<void>((resolve, reject) => {
    const current = document.querySelector<HTMLScriptElement>(`script[src="${src}"]`)
    if (current) {
      if ((current as any).__loaded) { resolve(); return }
      current.addEventListener('load', () => resolve(), { once: true })
      current.addEventListener('error', () => reject(new Error(`Failed to load ${src}`)), { once: true })
      return
    }
    const script = document.createElement('script')
    script.src = src
    script.async = true
    script.onload = () => { (script as any).__loaded = true; resolve() }
    script.onerror = () => reject(new Error(`Failed to load ${src}`))
    document.head.appendChild(script)
  })

  _scriptPromises.set(src, promise)
  return promise
}

/** Load BokehJS lazily from /assets. Resolves once on first call; cached thereafter. */
export function loadBokeh(): Promise<BokehNamespace> {
  if (window.Bokeh?.ColumnDataSource && window.Bokeh?.Plotting) {
    return Promise.resolve(window.Bokeh)
  }

  if (!_bokehPromise) {
    _bokehPromise = (async () => {
      let lastError: unknown = null
      for (const base of getBokehAssetBases()) {
        try {
          for (const file of BOKEH_FILES) {
            await loadScriptOnce(`${base}/${file}`)
          }
          const Bokeh = window.Bokeh
          if (!Bokeh?.ColumnDataSource || !Bokeh?.Plotting) {
            throw new Error('Bokeh namespace missing required constructors')
          }
          return Bokeh
        } catch (err) {
          lastError = err
          // reset per-file promises so next base gets a clean attempt
          for (const file of BOKEH_FILES) {
            _scriptPromises.delete(`${base}/${file}`)
          }
        }
      }
      throw lastError ?? new Error('Unable to load BokehJS from known asset locations')
    })()
  }

  return _bokehPromise
}

export function createLineSource(Bokeh: BokehNamespace): any {
  return new Bokeh.ColumnDataSource({ data: { x: [] as number[], y: [] as number[] } })
}
