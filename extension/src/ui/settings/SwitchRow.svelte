<script lang="ts">
  /** One settings row with a switch (`.frow` + `.switch`, design spec §3 "Settings"). */
  interface Props {
    label: string;
    sub?: string | null;
    value: boolean;
    disabled?: boolean;
    onchange: (value: boolean) => void;
  }

  let { label, sub = null, value, disabled = false, onchange }: Props = $props();
</script>

<div class="frow">
  <div class="k">{label}{#if sub}<span class="sub">{sub}</span>{/if}</div>
  <div class="v"><button class="switch" class:on={value} role="switch" aria-checked={value} aria-label={label} {disabled} onclick={() => onchange(!value)}></button></div>
</div>
