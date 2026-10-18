<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from 'wxt/browser';
  import { t } from '../../i18n';
  import { useSurface } from '../context';
  import { loadLicenses, type Licenses } from './licenses';

  /** Settings › About: the version, the engine, and the licenses of what the extension includes. */
  const { engine } = useSurface();
  let coreVersion = $state<string | null>(null);
  let licenses = $state<Licenses | null | 'loading'>('loading');
  let open = $state<string | null>(null);
  const version = manifestVersion();

  function manifestVersion(): string {
    try {
      return browser.runtime.getManifest().version;
    } catch {
      return '1.1.0';
    }
  }

  function licensesUrl(): string {
    try {
      // Written by the build (tools/licenses.ts), so WXT's list of public paths doesn't know it.
      return (browser.runtime.getURL as (path: string) => string)('/licenses.json');
    } catch {
      return '/licenses.json';
    }
  }

  onMount(async () => {
    const [hello, list] = await Promise.all([engine.hello().catch(() => null), loadLicenses(licensesUrl())]);
    coreVersion = hello?.coreVersion ?? null;
    licenses = list;
  });

  const versionLine = $derived(coreVersion ? `${t('settings.version', version)} · ${t('settings.engine', coreVersion)}` : t('settings.version', version));
</script>

{#snippet entry(key: string, title: string, summary: string, text: string)}
  <div class="frow license">
    <div class="k">{title}{#if summary}<span class="sub">{summary}</span>{/if}</div>
    {#if text}
      <div class="v">
        <button class="btn small text" aria-expanded={open === key} aria-controls="lic-{key}" onclick={() => (open = open === key ? null : key)}>
          {open === key ? t('settings.licenses.hide') : t('settings.licenses.show')}
        </button>
      </div>
    {/if}
  </div>
  {#if text && open === key}<pre class="license-text" id="lic-{key}" tabindex="-1">{text}</pre>{/if}
{/snippet}

<h2 class="sr-only">{t('settings.about')}</h2>
<div class="group">
  <div class="frow about">
    <span class="mini-disc small" aria-hidden="true"><span class="ic ic-mark"></span></span>
    <div class="k">{t('app.name')}<span class="sub tnum">{versionLine}</span></div>
  </div>
</div>
<div class="gfoot">{t('settings.about.line')}</div>

<h3 class="ghead">{t('settings.licenses')}</h3>
{#if licenses === 'loading'}
  <div class="gfoot">{t('settings.licenses.loading')}</div>
{:else if licenses === null}
  <div class="gfoot">{t('settings.licenses.none')}</div>
{:else}
  <div class="group licenses">
    {#each licenses.notices as notice, i (notice.title)}
      {@render entry(`n${i}`, notice.title, notice.summary, notice.text)}
    {/each}
    {#each licenses.packages as pkg, i (pkg.name)}
      {@render entry(`p${i}`, `${pkg.name} ${pkg.version}`, pkg.license, pkg.text)}
    {/each}
    {#if licenses.crates.length}
      {@render entry(
        'crates',
        t('settings.licenses.crates'),
        t('settings.licenses.crates.sub'),
        licenses.crates.map((c) => `${c.name} ${c.version} — ${c.license ?? '?'}`).join('\n'),
      )}
    {/if}
  </div>
  <div class="gfoot">{t('settings.licenses.footer')}</div>
{/if}
