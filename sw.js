const CACHE_NAME = 'eveole-app-v6'; // Changement de version pour forcer la mise à jour
const urlsToCache = [
  './',
  './index.html',
  'static/icon.png',
  'static/db.json',
  'manifest.json'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
  );
});

self.addEventListener('fetch', event => {
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});