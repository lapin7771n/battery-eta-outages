import { DurableObject } from "cloudflare:workers";

// Kept empty so the v1 migration that created it stays valid until a deleted_classes migration removes it.
export class Gate extends DurableObject<Env> {}
