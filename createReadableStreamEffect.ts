import { Stream, pipe } from 'effect';
import Effect from  'effect/Effect';
import { TextDecoder } from 'util';

export function consumeReadableStream(
  stream: ReadableStream<Uint8Array>,
  callback: (chunk: string) => void,
  signal: AbortSignal
): Effect.Effect<void, void, never> {
  const decoder = new TextDecoder();

  const abortEffect = Effect.async<void, void>((resolve) => {
    const onAbort = () => resolve(Effect.succeed<void>(undefined));  // Use Effect.unit here
    signal.addEventListener("abort", onAbort, { once: true });
  
    // Wrap the cleanup in Effect.sync to match the expected return type
    return Effect.sync(() => {
      signal.removeEventListener("abort", onAbort);
    }) as Effect.Effect<void, never, never>;
  });


  const streamEffect = pipe(
    Stream.fromReadableStream(() => stream, (_error) => Effect.fail<never>(undefined as never)),
    Stream.mapEffect((chunk) =>
      Effect.sync(() => callback(decoder.decode(chunk as Uint8Array)))
    ),
    Stream.runDrain
  );

  // Combine effects using Effect.race directly
  return Effect.race(streamEffect, abortEffect);
}