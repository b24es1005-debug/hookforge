import http from 'k6/http';
import { check } from 'k6';

export const options = {
    vus: 50,           // 50 Virtual Users firing concurrently
    duration: '10s',   // Sustain the attack for 10 seconds
};

export default function () {
    const payload = JSON.stringify({
        target_url: 'http://127.0.0.1:9999/dummy',
        payload: '{"load_test": true}'
    });

    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    // Hit our Go API
    const res = http.post('http://localhost:8080/api/v1/webhooks', payload, params);

    // Verify the API didn't crash and returned 202 Accepted
    check(res, { 'is status 202': (r) => r.status === 202 });
}
