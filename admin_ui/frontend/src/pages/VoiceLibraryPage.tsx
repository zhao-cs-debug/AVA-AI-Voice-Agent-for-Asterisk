import { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import yaml from 'js-yaml';
import { toast } from 'sonner';
import { Loader2, Music2, Pencil, Play, Plus, RefreshCw, Save, Trash2 } from 'lucide-react';
import { sanitizeConfigForSave } from '../utils/configSanitizers';

type VoiceSummary = {
    voice_id: string;
    display_name: string;
    status: string;
    active_revision_id?: string;
    latest_hifi_id?: string | null;
    prompt_text?: string;
    reference_audio_sha256?: string;
    language?: string;
    tags?: string[];
    metadata?: Record<string, unknown>;
    created_at?: string;
    updated_at?: string;
};

const emptyCreateForm = {
    display_name: '',
    prompt_text: '',
    language: 'zh-CN',
    tags: '',
    metadata: '{}',
};

const readFileAsBase64 = (file: File) => new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
        const result = String(reader.result || '');
        resolve(result.includes(',') ? result.split(',').pop() || '' : result);
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
});

const formatApiError = (err: any) => {
    const detail = err.response?.data?.detail || err.message || '';
    if (String(detail).includes('audio_decode_failed')) {
        return '音频生成成功但解码失败，请检查 B 服务器 ffmpeg/音频转码环境。';
    }
    return detail || 'Request failed';
};

const VoiceLibraryPage = () => {
    const [voices, setVoices] = useState<VoiceSummary[]>([]);
    const [loading, setLoading] = useState(true);
    const [creating, setCreating] = useState(false);
    const [previewingId, setPreviewingId] = useState<string | null>(null);
    const [deletingId, setDeletingId] = useState<string | null>(null);
    const [editingId, setEditingId] = useState<string | null>(null);
    const [createForm, setCreateForm] = useState(emptyCreateForm);
    const [audioFile, setAudioFile] = useState<File | null>(null);
    const [previewText, setPreviewText] = useState('您好，这是一段音色试听。');
    const [audioUrl, setAudioUrl] = useState<string | null>(null);
    const [contexts, setContexts] = useState<string[]>([]);
    const [selectedContext, setSelectedContext] = useState('');

    const activeVoiceByContext = useMemo(() => {
        return selectedContext ? voices.find((v) => v.status === 'ready') : undefined;
    }, [selectedContext, voices]);

    useEffect(() => {
        fetchVoices();
        fetchContexts();
        return () => {
            if (audioUrl) URL.revokeObjectURL(audioUrl);
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const fetchVoices = async () => {
        setLoading(true);
        try {
            const res = await axios.get('/api/voice-library');
            setVoices(Array.isArray(res.data?.items) ? res.data.items : []);
        } catch (err: any) {
            toast.error('Failed to load voice library', { description: formatApiError(err) });
        } finally {
            setLoading(false);
        }
    };

    const fetchContexts = async () => {
        try {
            const res = await axios.get('/api/config/yaml');
            const parsed = yaml.load(res.data.content || '') as any;
            const names = Object.keys(parsed?.contexts || {}).sort();
            setContexts(names);
            setSelectedContext(names[0] || '');
        } catch {
            setContexts([]);
        }
    };

    const parseTags = () => createForm.tags
        .split(',')
        .map((tag) => tag.trim())
        .filter(Boolean);

    const parseMetadata = () => {
        const raw = createForm.metadata.trim();
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
            throw new Error('metadata must be a JSON object');
        }
        return parsed;
    };

    const createVoice = async () => {
        if (!audioFile) {
            toast.error('Please select a .wav reference audio file');
            return;
        }
        if (!audioFile.name.toLowerCase().endsWith('.wav')) {
            toast.error('Only .wav files are supported');
            return;
        }
        if (audioFile.size > 20 * 1024 * 1024) {
            toast.error('Reference audio must be 20MB or smaller');
            return;
        }

        setCreating(true);
        try {
            const wavBase64 = await readFileAsBase64(audioFile);
            const res = await axios.post('/api/voice-library', {
                display_name: createForm.display_name.trim(),
                prompt_text: createForm.prompt_text.trim(),
                wav_base64: wavBase64,
                wav_format: 'wav',
                source_filename: audioFile.name,
                language: createForm.language || 'zh-CN',
                tags: parseTags(),
                metadata: parseMetadata(),
            });
            toast.success('Voice created', { description: res.data?.voice_id });
            setCreateForm(emptyCreateForm);
            setAudioFile(null);
            await fetchVoices();
        } catch (err: any) {
            toast.error('Failed to create voice', { description: formatApiError(err) });
        } finally {
            setCreating(false);
        }
    };

    const updateVoice = async (voice: VoiceSummary) => {
        const displayName = window.prompt('Display name', voice.display_name);
        if (!displayName) return;
        const existingTags = Array.isArray(voice.tags) ? voice.tags.join(', ') : '';
        const existingMetadata = voice.metadata && typeof voice.metadata === 'object'
            ? JSON.stringify(voice.metadata, null, 2)
            : '';
        const tagsRaw = window.prompt('Tags, comma separated. Leave blank to keep unchanged.', existingTags);
        const metadataRaw = window.prompt('Metadata JSON object. Leave blank to keep unchanged.', existingMetadata);
        setEditingId(voice.voice_id);
        try {
            const payload: Record<string, any> = { display_name: displayName.trim() };
            if (tagsRaw !== null && tagsRaw.trim()) {
                payload.tags = tagsRaw.split(',').map((tag) => tag.trim()).filter(Boolean);
            }
            if (metadataRaw !== null && metadataRaw.trim()) {
                const metadata = JSON.parse(metadataRaw);
                if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
                    throw new Error('metadata must be a JSON object');
                }
                payload.metadata = metadata;
            }
            await axios.patch(`/api/voice-library/${encodeURIComponent(voice.voice_id)}`, {
                ...payload,
            });
            toast.success('Voice updated');
            await fetchVoices();
        } catch (err: any) {
            toast.error('Failed to update voice', { description: formatApiError(err) });
        } finally {
            setEditingId(null);
        }
    };

    const deleteVoice = async (voice: VoiceSummary) => {
        if (!window.confirm(`Delete voice "${voice.display_name}"?`)) return;
        setDeletingId(voice.voice_id);
        try {
            await axios.delete(`/api/voice-library/${encodeURIComponent(voice.voice_id)}`);
            toast.success('Voice deleted');
            await fetchVoices();
        } catch (err: any) {
            toast.error('Failed to delete voice', { description: formatApiError(err) });
        } finally {
            setDeletingId(null);
        }
    };

    const previewVoice = async (voice: VoiceSummary) => {
        setPreviewingId(voice.voice_id);
        try {
            const res = await axios.post(`/api/voice-library/${encodeURIComponent(voice.voice_id)}/preview`, {
                target_text: previewText,
                response_audio_format: 'mp3_base64',
            });
            const binary = atob(res.data.audio_base64 || '');
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
            const blob = new Blob([bytes], { type: res.data.audio_format === 'mp3' ? 'audio/mpeg' : 'audio/basic' });
            if (audioUrl) URL.revokeObjectURL(audioUrl);
            const url = URL.createObjectURL(blob);
            setAudioUrl(url);
            setTimeout(() => {
                const audio = new Audio(url);
                audio.play().catch(() => undefined);
            }, 0);
        } catch (err: any) {
            toast.error('Failed to preview voice', { description: formatApiError(err) });
        } finally {
            setPreviewingId(null);
        }
    };

    const setDefaultVoice = async (voice: VoiceSummary) => {
        if (!selectedContext) {
            toast.error('Please select a context first');
            return;
        }
        try {
            const res = await axios.get('/api/config/yaml');
            const parsed = (yaml.load(res.data.content || '') as any) || {};
            parsed.contexts = parsed.contexts || {};
            parsed.contexts[selectedContext] = parsed.contexts[selectedContext] || {};
            parsed.contexts[selectedContext].default_voice = {
                voice_id: voice.voice_id,
                voice_revision_id: voice.active_revision_id,
                hifi_id: voice.latest_hifi_id || undefined,
            };
            await axios.post('/api/config/yaml', { content: yaml.dump(sanitizeConfigForSave(parsed)) });
            toast.success('Default voice saved', { description: `Context: ${selectedContext}` });
        } catch (err: any) {
            toast.error('Failed to save default voice', { description: formatApiError(err) });
        }
    };

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold flex items-center gap-2">
                        <Music2 className="w-6 h-6" />
                        HiFi Voice Library
                    </h1>
                    <p className="text-sm text-muted-foreground mt-1">
                        Upload reference WAV audio, preview B/C generated speech, and assign a context default voice.
                    </p>
                </div>
                <button onClick={fetchVoices} className="px-3 py-2 rounded-md border border-border hover:bg-accent flex items-center gap-2 text-sm">
                    <RefreshCw className="w-4 h-4" />
                    Refresh
                </button>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-1 p-4 rounded-lg border border-border bg-card/50 space-y-3">
                    <h2 className="font-semibold flex items-center gap-2"><Plus className="w-4 h-4" /> New Voice</h2>
                    <input className="w-full h-9 rounded-md border border-input bg-transparent px-3 text-sm" placeholder="Display name" value={createForm.display_name} onChange={(e) => setCreateForm({ ...createForm, display_name: e.target.value })} />
                    <textarea className="w-full min-h-[96px] rounded-md border border-input bg-transparent p-3 text-sm" placeholder="Prompt text matching the reference audio" value={createForm.prompt_text} onChange={(e) => setCreateForm({ ...createForm, prompt_text: e.target.value })} />
                    <input className="w-full h-9 rounded-md border border-input bg-transparent px-3 text-sm" placeholder="Language, e.g. zh-CN" value={createForm.language} onChange={(e) => setCreateForm({ ...createForm, language: e.target.value })} />
                    <input className="w-full h-9 rounded-md border border-input bg-transparent px-3 text-sm" placeholder="Tags, comma separated" value={createForm.tags} onChange={(e) => setCreateForm({ ...createForm, tags: e.target.value })} />
                    <textarea className="w-full min-h-[72px] rounded-md border border-input bg-transparent p-3 text-sm font-mono" value={createForm.metadata} onChange={(e) => setCreateForm({ ...createForm, metadata: e.target.value })} />
                    <input type="file" accept=".wav,audio/wav,audio/x-wav" onChange={(e) => setAudioFile(e.target.files?.[0] || null)} className="w-full text-sm" />
                    <button disabled={creating} onClick={createVoice} className="w-full px-3 py-2 rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-60 flex items-center justify-center gap-2 text-sm">
                        {creating ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                        Create HiFi Cache
                    </button>
                </div>

                <div className="lg:col-span-2 space-y-4">
                    <div className="p-4 rounded-lg border border-border bg-card/50">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <input className="h-9 rounded-md border border-input bg-transparent px-3 text-sm" value={previewText} onChange={(e) => setPreviewText(e.target.value)} placeholder="Preview text" />
                            <select className="h-9 rounded-md border border-input bg-transparent px-3 text-sm" value={selectedContext} onChange={(e) => setSelectedContext(e.target.value)}>
                                <option value="">Select context for default voice</option>
                                {contexts.map((name) => <option key={name} value={name}>{name}</option>)}
                            </select>
                        </div>
                        {activeVoiceByContext && <p className="text-xs text-muted-foreground mt-2">Ready voices can be assigned to the selected context.</p>}
                    </div>

                    {loading ? (
                        <div className="p-8 text-center text-muted-foreground">Loading voice library...</div>
                    ) : voices.length === 0 ? (
                        <div className="p-8 rounded-lg border border-dashed border-border text-center text-muted-foreground">
                            No voices yet. Create one from a reference WAV file.
                        </div>
                    ) : (
                        <div className="space-y-3">
                            {voices.map((voice) => (
                                <div key={voice.voice_id} className="p-4 rounded-lg border border-border bg-card/50">
                                    <div className="flex items-start justify-between gap-4">
                                        <div className="min-w-0">
                                            <div className="flex items-center gap-2">
                                                <h3 className="font-semibold truncate">{voice.display_name}</h3>
                                                <span className="text-xs px-2 py-0.5 rounded-full bg-primary/10 text-primary">{voice.status}</span>
                                            </div>
                                            <p className="text-xs text-muted-foreground break-all mt-1">{voice.voice_id}</p>
                                            <p className="text-sm mt-2 line-clamp-2">{voice.prompt_text || 'No prompt text returned'}</p>
                                            <div className="grid grid-cols-1 md:grid-cols-2 gap-1 mt-3 text-xs text-muted-foreground">
                                                <span>Revision: {voice.active_revision_id || '-'}</span>
                                                <span>HiFi: {voice.latest_hifi_id || '-'}</span>
                                                <span>Language: {voice.language || 'zh-CN'}</span>
                                                <span>Tags: {Array.isArray(voice.tags) && voice.tags.length > 0 ? voice.tags.join(', ') : '-'}</span>
                                                <span>Created: {voice.created_at || '-'}</span>
                                                <span>Updated: {voice.updated_at || '-'}</span>
                                            </div>
                                            {voice.metadata && Object.keys(voice.metadata).length > 0 ? (
                                                <pre className="mt-2 max-h-24 overflow-auto rounded bg-muted/40 p-2 text-xs text-muted-foreground">
                                                    {JSON.stringify(voice.metadata, null, 2)}
                                                </pre>
                                            ) : null}
                                        </div>
                                        <div className="flex flex-wrap justify-end gap-2">
                                            <button onClick={() => previewVoice(voice)} disabled={previewingId === voice.voice_id} className="px-2 py-1.5 rounded-md border border-border hover:bg-accent text-xs flex items-center gap-1">
                                                {previewingId === voice.voice_id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                                                Preview
                                            </button>
                                            <button onClick={() => setDefaultVoice(voice)} className="px-2 py-1.5 rounded-md border border-border hover:bg-accent text-xs">Set Default</button>
                                            <button onClick={() => updateVoice(voice)} disabled={editingId === voice.voice_id} className="px-2 py-1.5 rounded-md border border-border hover:bg-accent text-xs flex items-center gap-1">
                                                <Pencil className="w-3 h-3" />
                                                Edit
                                            </button>
                                            <button onClick={() => deleteVoice(voice)} disabled={deletingId === voice.voice_id} className="px-2 py-1.5 rounded-md border border-destructive/40 text-destructive hover:bg-destructive/10 text-xs flex items-center gap-1">
                                                {deletingId === voice.voice_id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                                                Delete
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default VoiceLibraryPage;
