import { z } from "zod";
export const snapshotPolicySchema = z
  .object({
    allowEmpty: z.boolean().optional(),
    minimumRows: z.number().int().min(0).optional(),
    maximumDropPercent: z.number().min(0).max(100).optional(),
    maximumAgeHours: z.number().positive().optional(),
  })
  .strict();
