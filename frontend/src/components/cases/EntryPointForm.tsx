/**
 * Entry point form component
 *
 * Dynamic input form based on entry point type.
 * Includes autocomplete for corporation and meta_ad types.
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { autocompleteEntities, type AutocompleteSuggestion } from '@/api';

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
  const supportsAutocomplete = type === 'corporation' || type === 'meta_ad';
  
  const [suggestions, setSuggestions] = useState<AutocompleteSuggestion[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<NodeJS.Timeout | null>(null);

  // Fetch suggestions with debounce
  const fetchSuggestions = useCallback(async (query: string) => {
    if (!supportsAutocomplete || query.length < 2) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }

    setIsLoading(true);
    try {
      const entityType = type === 'meta_ad' ? 'sponsor,organization' : 'organization';
      const result = await autocompleteEntities({ q: query, limit: 8, type: entityType });
      setSuggestions(result.suggestions ?? []);
      setShowSuggestions((result.suggestions?.length ?? 0) > 0);
      setSelectedIndex(-1);
    } catch (error) {
      console.error('Autocomplete error:', error);
      setSuggestions([]);
    } finally {
      setIsLoading(false);
    }
  }, [supportsAutocomplete, type]);

  // Debounced input handler
  const handleInputChange = (newValue: string) => {
    onChange(newValue);
    
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    
    debounceRef.current = setTimeout(() => {
      fetchSuggestions(newValue);
    }, 200);
  };

  // Select a suggestion
  const selectSuggestion = (suggestion: AutocompleteSuggestion) => {
    onChange(suggestion.name ?? '');
    setSuggestions([]);
    setShowSuggestions(false);
    setSelectedIndex(-1);
    inputRef.current?.focus();
  };

  // Keyboard navigation
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showSuggestions || suggestions.length === 0) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedIndex((prev) => 
          prev < suggestions.length - 1 ? prev + 1 : 0
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedIndex((prev) => 
          prev > 0 ? prev - 1 : suggestions.length - 1
        );
        break;
      case 'Enter':
        e.preventDefault();
        if (selectedIndex >= 0 && selectedIndex < suggestions.length) {
          selectSuggestion(suggestions[selectedIndex]);
        }
        break;
      case 'Escape':
        setShowSuggestions(false);
        setSelectedIndex(-1);
        break;
    }
  };

  // Close suggestions when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(e.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(e.target as Node)
      ) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, []);

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
        <div className="autocomplete-container">
          <input
            ref={inputRef}
            type={type === 'url' ? 'url' : 'text'}
            value={value}
            onChange={(e) => handleInputChange(e.target.value)}
            onKeyDown={handleKeyDown}
            onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
            placeholder={placeholder}
            className="entry-input"
            autoComplete="off"
          />
          
          {isLoading && (
            <div className="autocomplete-loading">
              <span className="loading-spinner" />
            </div>
          )}
          
          {showSuggestions && suggestions.length > 0 && (
            <div ref={suggestionsRef} className="autocomplete-dropdown">
              {suggestions.map((suggestion, index) => (
                <div
                  key={`${suggestion.name}-${index}`}
                  className={`autocomplete-item ${index === selectedIndex ? 'selected' : ''}`}
                  onClick={() => selectSuggestion(suggestion)}
                  onMouseEnter={() => setSelectedIndex(index)}
                >
                  <span className="suggestion-name">{suggestion.name}</span>
                  <span className="suggestion-meta">
                    {suggestion.jurisdiction && (
                      <span className="suggestion-jurisdiction">{suggestion.jurisdiction}</span>
                    )}
                    <span className="suggestion-type">{suggestion.entity_type}</span>
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <style>{`
        .entry-point-form {
          margin-top: var(--spacing-md);
        }

        .entry-point-form .description {
          margin-bottom: var(--spacing-sm);
          color: var(--text-secondary);
        }

        .autocomplete-container {
          position: relative;
        }

        .entry-input {
          width: 100%;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          font-size: 1rem;
          font-family: inherit;
          box-sizing: border-box;
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

        .autocomplete-loading {
          position: absolute;
          right: 10px;
          top: 50%;
          transform: translateY(-50%);
        }

        .loading-spinner {
          display: inline-block;
          width: 16px;
          height: 16px;
          border: 2px solid var(--border-color);
          border-top-color: var(--primary);
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
        }

        @keyframes spin {
          to { transform: rotate(360deg); }
        }

        .autocomplete-dropdown {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: #ffffff;
          border: 1px solid #d1d5db;
          border-top: none;
          border-radius: 0 0 6px 6px;
          box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
          max-height: 280px;
          overflow-y: auto;
          z-index: 1000;
        }

        .autocomplete-item {
          padding: 10px 14px;
          cursor: pointer;
          display: flex;
          justify-content: space-between;
          align-items: center;
          background: #ffffff;
          border-bottom: 1px solid #e5e7eb;
          transition: background-color 0.1s ease;
        }

        .autocomplete-item:last-child {
          border-bottom: none;
        }

        .autocomplete-item:hover,
        .autocomplete-item.selected {
          background: #f3f4f6;
        }

        .suggestion-name {
          font-weight: 500;
          color: #1f2937;
          flex: 1;
          margin-right: 12px;
        }

        .suggestion-meta {
          display: flex;
          gap: 8px;
          font-size: 0.8rem;
          align-items: center;
          flex-shrink: 0;
        }

        .suggestion-jurisdiction {
          color: #6b7280;
          font-weight: 500;
        }

        .suggestion-type {
          color: #2563eb;
          text-transform: capitalize;
          background: #eff6ff;
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.7rem;
          font-weight: 600;
        }
      `}</style>
    </div>
  );
}
