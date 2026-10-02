import { requireAuthorization } from '../../../../lib/authorization';
import { portfolioResponse } from '../../../../lib/portfolio-http';
export const runtime = 'nodejs';
// Snapshot recovery: stream cursors captured before the owner's PostgreSQL snapshot (ADR 0007).
export function GET(request: Request) {
  return portfolioResponse(request, async () => (await requireAuthorization(request)).recovery());
}
