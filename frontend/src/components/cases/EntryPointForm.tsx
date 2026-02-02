/**
 * Entry point form component
 *
 * Dynamic input form based on entry point type.
 */

interface EntryPointFormProps {
  type: 'meta_ad' | 'corporation' | 'url' | 'text';
  value: string;
  onChange: (value: string) => void;
  description: string;
  placeholder: string;
}

export function EntryPointForm({
  type,
  value,
  onChange,
  description,
  placeholder,
}: EntryPointFormProps) {
  const isTextArea = type === 'text';

  return (
    <div className="entry-point-form">
      <p className="description">{description}</p>
      {isTextArea ? (
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          rows={8}
          className="entry-input"
        />
      ) : (
        <input
          type={type === 'url' ? 'url' : 'text'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className="entry-input"
        />
      )}

      <style>{`
        .entry-point-form {
          margin-top: var(--spacing-md);
        }

        .entry-point-form .description {
          margin-bottom: var(--spacing-sm);
          color: var(--text-secondary);
        }

        .entry-input {
          width: 100%;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          font-size: 1rem;
          font-family: inherit;
        }

        .entry-input:focus {
          outline: none;
          border-color: var(--primary);
          box-shadow: 0 0 0 2px rgba(var(--primary-rgb), 0.2);
        }

        textarea.entry-input {
          resize: vertical;
          min-height: 150px;
        }
      `}</style>
    </div>
  );
}
