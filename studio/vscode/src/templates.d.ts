export interface Template {
  id: string;
  label: string;
  detail: string;
  make: (name: string) => Record<string, unknown>;
}

export const TEMPLATES: Template[];
