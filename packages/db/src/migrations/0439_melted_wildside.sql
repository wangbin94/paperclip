CREATE TABLE "issue_operator_notes" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid NOT NULL,
	"issue_id" uuid NOT NULL,
	"author_user_id" text NOT NULL,
	"request_id" text NOT NULL,
	"body" text NOT NULL,
	"body_sha256" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "issue_operator_notes_request_uq" UNIQUE("issue_id","author_user_id","request_id")
);
--> statement-breakpoint
ALTER TABLE "issue_operator_notes" ADD CONSTRAINT "issue_operator_notes_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "issue_operator_notes" ADD CONSTRAINT "issue_operator_notes_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "issue_operator_notes_company_issue_created_idx" ON "issue_operator_notes" USING btree ("company_id","issue_id","created_at");