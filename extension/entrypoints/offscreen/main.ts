import { startEngineHost } from '@/src/engine/factory';

// The engine host on Chromium (extension plan §3.1): the service worker only routes.
startEngineHost();
