CREATE TABLE "issue_task_operation_receipts" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid NOT NULL,
	"issue_id" uuid NOT NULL,
	"operation" text NOT NULL,
	"actor_type" text NOT NULL,
	"actor_id" text NOT NULL,
	"request_id" text NOT NULL,
	"payload_digest" text NOT NULL,
	"decision" jsonb NOT NULL,
	"wake_idempotency_key" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "issue_task_operation_receipts" ADD CONSTRAINT "issue_task_operation_receipts_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "issue_task_operation_receipts" ADD CONSTRAINT "issue_task_operation_receipts_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "issue_task_operation_receipts_request_uq" ON "issue_task_operation_receipts" USING btree ("company_id","issue_id","actor_type","actor_id","request_id");--> statement-breakpoint
CREATE INDEX "issue_task_operation_receipts_company_issue_idx" ON "issue_task_operation_receipts" USING btree ("company_id","issue_id");