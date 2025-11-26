// schemas.ts
import { z } from 'zod';
// Version 3.24.0+

export const dateRangeSearchSchema = z.object({
	startDate: z.iso.date().default('2025-02-11'),
	endDate: z.iso.date().default(new Date().toJSON().slice(0, 10)),
	groupBy: z.enum(['date', 'hour', '30min', '5min']).default('date')
});
