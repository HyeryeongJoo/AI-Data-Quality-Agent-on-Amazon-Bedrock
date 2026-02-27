import { useState } from 'react';
import type { FlashbarProps } from '@cloudscape-design/components/flashbar';

export function useNotifications() {
  const [notifications, setNotifications] = useState<FlashbarProps.MessageDefinition[]>([]);

  const dismiss = (id: string) => {
    setNotifications(prev => prev.filter(n => n.id !== id));
  };

  const notify = (
    type: FlashbarProps.Type,
    content: string,
    dismissible = true,
    id?: string,
  ) => {
    const nid = id || `${Date.now()}`;
    setNotifications(prev => [
      ...prev.filter(n => n.id !== nid),
      { type, content, dismissible, id: nid, onDismiss: () => dismiss(nid) },
    ]);
  };

  return {
    notifications,
    notifySuccess: (msg: string) => notify('success', msg),
    notifyError: (msg: string) => notify('error', msg),
    notifyInfo: (msg: string) => notify('info', msg),
    notifyLoading: (msg: string, id?: string) => notify('info', msg, false, id),
    clearNotification: dismiss,
  };
}
