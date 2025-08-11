// src/components/ui/card.tsx
import React from 'react';
import './card.css';

export interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: 'glass' | 'glass-up' | 'glass-down' | 'glass-info';
  padding?: 'sm' | 'md' | 'lg';
  hoverable?: boolean;
}

const Card: React.FC<CardProps> = ({
  children,
  variant = 'glass',
  padding = 'md',
  hoverable = true,
  className = '',
  ...props
}) => {
  const baseClass = 'card-liquid';
  const variantClass = `card-${variant}`;
  const paddingClass = `card-padding-${padding}`;
  const hoverClass = hoverable ? 'card-hoverable' : '';

  return (
    <div
      className={`${baseClass} ${variantClass} ${paddingClass} ${hoverClass} ${className}`.trim()}
      {...props}
    >
      {children}
    </div>
  );
};

export default Card;