#!/usr/bin/env node
/**
 * Dev server for simpleMQ management UI
 *
 * - Serves static files from src/main/resources/META-INF/resources/
 * - Proxies /api/* requests to the Quarkus backend
 *
 * Usage:
 *   node devserver.js [port] [quarkus-origin]
 *
 * Defaults:
 *   port            = 3000
 *   quarkus-origin  = http://localhost:8080
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.argv[2]) || 3000;
const BACKEND = process.argv[3] || 'http://localhost:8080';
const STATIC_DIR = path.join(__dirname, 'src/main/resources/META-INF/resources');

const MIME = {
    '.html': 'text/html',
    '.css':  'text/css',
    '.js':   'application/javascript',
    '.json': 'application/json',
    '.png':  'image/png',
    '.svg':  'image/svg+xml',
    '.ico':  'image/x-icon',
};

const backendUrl = new URL(BACKEND);
const transport = backendUrl.protocol === 'https:' ? https : http;

function proxy(req, res) {
    const options = {
        hostname: backendUrl.hostname,
        port:     backendUrl.port || (backendUrl.protocol === 'https:' ? 443 : 80),
        path:     req.url,
        method:   req.method,
        headers:  { ...req.headers, host: backendUrl.host },
    };

    const proxyReq = transport.request(options, (proxyRes) => {
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res);
    });

    proxyReq.on('error', (err) => {
        console.error('[proxy error]', err.message);
        res.writeHead(502);
        res.end(`Backend unavailable: ${err.message}`);
    });

    req.pipe(proxyReq);
}

function serveStatic(req, res) {
    let urlPath = req.url.split('?')[0];
    if (urlPath === '/') urlPath = '/index.html';

    const filePath = path.join(STATIC_DIR, urlPath);

    // Prevent path traversal
    if (!filePath.startsWith(STATIC_DIR)) {
        res.writeHead(403);
        res.end('Forbidden');
        return;
    }

    fs.readFile(filePath, (err, data) => {
        if (err) {
            res.writeHead(404);
            res.end('Not found');
            return;
        }
        const ext = path.extname(filePath);
        res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
        res.end(data);
    });
}

const server = http.createServer((req, res) => {
    if (req.url.startsWith('/api/')) {
        proxy(req, res);
    } else {
        serveStatic(req, res);
    }
});

server.listen(PORT, () => {
    console.log(`simpleMQ dev server running at http://localhost:${PORT}`);
    console.log(`Proxying /api/* → ${BACKEND}`);
});
